package ai.chronon.spark.stats.drift

import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.SerdeUtils.compactSerializer
import ai.chronon.api._
import ai.chronon.observability.{Cardinality, TileSummaryKey}
import ai.chronon.online.Api
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.PartitionRunner
import ai.chronon.spark.stats.drift.Expressions.CardinalityExpression
import ai.chronon.spark.stats.drift.Expressions.SummaryExpression
import ai.chronon.spark.stats.drift.Expressions.TileRow
import ai.chronon.spark.udafs.ArrayApproxDistinct
import ai.chronon.spark.udafs.ArrayStringHistogramAggregator
import ai.chronon.spark.udafs.HistogramAggregator
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.types
import org.slf4j.LoggerFactory

import java.io.Serializable
import java.nio.charset.Charset
import scala.concurrent.Await
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class Summarizer(api: Api,
                 confPath: String,
                 timeColumn: Option[String] = None,
                 val sliceColumns: Option[Seq[String]] = None,
                 derivedColumns: Option[Map[String, String]] = None,
                 includeColumns: Option[Seq[String]] = None,
                 excludeColumns: Option[Seq[String]] = None,
                 val tileSize: Window = new Window(5, TimeUnit.MINUTES),
                 cardinalityThreshold: Int = 100)(implicit val tu: TableUtils)
    extends Serializable {

  // Initialize the logger
  private val logger = LoggerFactory.getLogger(getClass)

  // prune down to the set of columns to summarize + validations
  private def prepare(df: DataFrame): (DataFrame, DataFrame) = {

    logger.info(s"Original schema:\n${df.schema}".green)

    val derivedDf = derivedColumns
      .map { dc =>
        val derivedColumns = dc.map { case (k, v) => s"$v as `$k`" }.toSeq
        // original columns that are not colliding with derived map
        // all valid slice columns will be included into the result
        val originalColumns = df.columns.filterNot(dc.keySet.contains)
        val derivedDf = df.selectExpr(originalColumns ++ derivedColumns: _*)
        logger.info(s"Schema after derivations:\n${derivedDf.schema}".green)
        derivedDf
      }
      .getOrElse(df)

    if (includeColumns.nonEmpty) {
      val unknownColumns = includeColumns.get.filterNot(derivedDf.columns.contains)
      assert(unknownColumns.isEmpty, s"Unknown columns to include: ${unknownColumns.mkString(", ")}")
    }

    if (excludeColumns.nonEmpty) {
      val unknownColumns = excludeColumns.get.filterNot(derivedDf.columns.contains)
      assert(unknownColumns.isEmpty, s"Unknown columns to exclude: ${unknownColumns.mkString(", ")}")
    }

    if (sliceColumns.nonEmpty) {
      assert(sliceColumns.get.length <= 1, "We only support one slice column for now")
      val unknownColumns = sliceColumns.get.filterNot(derivedDf.columns.contains)
      assert(unknownColumns.isEmpty, s"Unknown slice columns: ${unknownColumns.mkString(", ")}")
    }

    val derivedDfWithTile = injectTile(derivedDf)

    val summaryColumns = (includeColumns
      .getOrElse(derivedDfWithTile.columns.toSeq)
      .filterNot(excludeColumns.getOrElse(Seq.empty).contains) ++
      sliceColumns.getOrElse(Seq.empty)).distinct

    val cardinalityColumns = summaryColumns
      .filterNot((tu.partitionColumn +: timeColumn.toSeq +: Constants.TileColumn).contains)

    assert(cardinalityColumns.nonEmpty, "No columns selected for cardinality estimation")
    assert(summaryColumns.nonEmpty, "No columns selected for summarization")

    val cardinalityInputDf = derivedDfWithTile.select(cardinalityColumns.head, cardinalityColumns.tail: _*)
    val summaryInputDf = derivedDfWithTile.select(summaryColumns.head, summaryColumns.tail: _*)
    cardinalityInputDf -> summaryInputDf
  }

  def keys(df: DataFrame): Seq[String] = {
    val partitionCol =
      if (df.columns.contains(tu.partitionColumn))
        Some(s"${tu.partitionColumn}")
      else None

    val sliceCol = sliceColumns
      .getOrElse(Seq.empty)
      .headOption

    Seq(Constants.TileColumn) ++ partitionCol ++ sliceCol
  }

  // inject a tile column into the data frame to compute summaries within.
  private def injectTile(derivedDf: DataFrame): DataFrame = {
    val derivedCols = derivedDf.columns
    assert(
      !derivedCols.contains(Constants.TileColumn),
      s"Time column ${Constants.TileColumn} is reserved. Please use a different name. columns are: ${derivedCols.mkString(", ")}"
    )

    // adds a time based tile column for a given time expression in milliseconds
    def addTileCol(ts: String): DataFrame = {
      val tileExpr = {
        val millis = tileSize.millis
        s"CAST((round($ts/$millis) * $millis) AS LONG)" // round to the nearest tile
      }
      logger.info(derivedDf.columns.mkString(", ").yellow)
      logger.info("tile_expr: ".yellow + tileExpr)
      val result = derivedDf.withColumn(Constants.TileColumn, expr(tileExpr))

      if (derivedCols.contains(ts)) result.drop(ts) else result
    }

    val timeCol = timeColumn.getOrElse(
      if (derivedCols.contains(Constants.TimeColumn)) Constants.TimeColumn
      else null
    )

    if (timeCol != null) {
      addTileCol(timeCol)
    } else if (derivedCols.contains(tu.partitionColumn)) {
      val conversionEx =
        s"unix_seconds(try_to_timestamp(${tu.partitionColumn}, '${tu.partitionSpec.format}')) * 1000"
      // no rounding if no millisTime - just tile by partition column
      logger.info("Ignoring tileSize since no time column specified, tiling by the partition instead".blue)
      derivedDf.withColumn(Constants.TileColumn, expr(conversionEx)).drop(tu.partitionColumn)
    } else {
      // nothing to groupBy using a dummy
      derivedDf.withColumn(Constants.TileColumn, lit(0.0))
    }
  }

  private def getOrComputeCardinalityMap(dataFrame: DataFrame): Map[String, Double] = {
    val kvStore = api.genKvStore

    // construct request
    val summaryDataset = Constants.TiledSummaryDataset
    val key = s"$confPath/column_cardinality_map"
    val charset = Charset.forName("UTF-8")
    val getRequest = GetRequest(key.getBytes(charset), summaryDataset)

    // fetch result
    val responseFuture = kvStore.get(getRequest)
    val response = Await.result(responseFuture, Constants.FetchTimeout)

    // if response is empty, compute cardinality map and put it
    response.values match {
      case Failure(exception) =>
        logger.error(s"Failed to fetch cardinality map from KVStore: ${exception.getMessage}".red)
        computeAndPutCardinalityMap(dataFrame)
      case Success(values) =>
        if (values == null || values.isEmpty) {
          logger.info("Cardinality map not found in KVStore, computing and putting it".yellow)
          computeAndPutCardinalityMap(dataFrame)
        } else {
          val mapBytes = values.maxBy(_.millis).bytes
          val gson = new com.google.gson.Gson()
          val cardinalityMapJson = new String(mapBytes, charset)
          logger.info(s"Cardinality map found in KVStore: $cardinalityMapJson".yellow)
          val cardinalityMap = gson.fromJson(cardinalityMapJson, classOf[java.util.Map[String, Double]])
          val result = cardinalityMap.toScala
          result
        }
    }
  }

  private val cardinalityMapKey = s"$confPath/column_cardinality_map"
  private def cardinalityMapGetRequest: GetRequest = {
    // construct request
    val summaryDataset = Constants.TiledSummaryDataset
    GetRequest(cardinalityMapKey.getBytes(Constants.DefaultCharset), summaryDataset)
  }

  private def computeAndPutCardinalityMap(dataFrame: DataFrame): Map[String, Double] = {
    val kvStore = api.genKvStore
    val getRequest = cardinalityMapGetRequest
    val dataset = getRequest.dataset
    val keyBytes = getRequest.keyBytes
    logger.info("Computing cardinality map".yellow)
    val cardinalityMap = buildCardinalityMap(dataFrame)
    // we use json to serialize this map - we convert to java map to simplify deps to gson
    val gson = new com.google.gson.Gson()
    val cardinalityMapJson = gson.toJson(cardinalityMap.toJava)
    val cardinalityMapBytes = cardinalityMapJson.getBytes(Constants.DefaultCharset)
    logger.info("Writing to kvstore @ " + s"$dataset[$cardinalityMapKey] = $cardinalityMapJson".yellow)
    val putRequest = PutRequest(keyBytes, cardinalityMapBytes, dataset)
    kvStore.create(dataset)
    kvStore.put(putRequest)
    cardinalityMap
  }

  private def buildCardinalityMap(dataFrame: DataFrame): Map[String, Double] = {
    val cardinalityInputDf = prepare(dataFrame)._1
    val exprs: Seq[(String, String)] = cardinalityInputDf.schema.fields
      .flatMap { f =>
        val ceTry = Try { CardinalityExpression(f.dataType).render(f.name) }
        ceTry match {
          case Failure(e) =>
            logger.info(
              s"Cannot compute cardinality of column ${f.name} with type ${f.dataType}. " +
                s"Error: ${e.getMessage}".red)
            None
          case Success(x) =>
            Some(x.select.get -> x.aggExpr)
        }
      }

    val inputTransformed = cardinalityInputDf.selectExpr(exprs.map(_._1): _*)
    val spark = dataFrame.sparkSession
    spark.udf.register("array_dbl_distinct", udaf(new ArrayApproxDistinct[Double]()))
    val aggregated = inputTransformed.selectExpr(exprs.map(_._2): _*)
    aggregated.schema.fields.map { f => f.name -> f.dataType }.toMap
    val counts = aggregated.collect().head.getValuesMap[Long](aggregated.columns).mapValues(_.toDouble).toMap
    logger.info(s"Counts for each field:\n  ${counts.mkString(",\n  ")}")

    // verify that all slices are low cardinality
    for (
      cols <- sliceColumns;
      col <- cols;
      count <- counts.get(col)
    ) {
      assert(count <= cardinalityThreshold, s"Slice column $col is high cardinality $count")
    }
    val cardinalityBlurb = counts
      .map { case (k, v) => s"  $k: $v, [${if (cardinalityThreshold < v) "high" else "low"}]".yellow }
      .mkString("\n")
    logger.info("Cardinality counts:".red + s"\n$cardinalityBlurb")
    counts
  }

  private def buildSummaryExpressions(inputDf: DataFrame, summaryInputDf: DataFrame): Seq[SummaryExpression] = {
    val cardinalityMap = getOrComputeCardinalityMap(inputDf)
    val excludedFields = Set(Constants.TileColumn, tu.partitionColumn, Constants.TimeColumn)
    summaryInputDf.schema.fields.filterNot { f => excludedFields.contains(f.name) }.flatMap { f =>
      val count = cardinalityMap(f.name + "_cardinality")
      val cardinality = if (count <= cardinalityThreshold) Cardinality.LOW else Cardinality.HIGH

      SummaryExpression.of(f.dataType, cardinality, f.name)
    }
  }

  private[spark] def computeSummaryDf(df: DataFrame): (DataFrame, Seq[SummaryExpression]) = {
    val summaryInputDf = prepare(df)._2
    val id = java.util.UUID.randomUUID().toString.replace('-', '_')

    val summaryInputViewName = s"summary_input_$id"
    summaryInputDf.createOrReplaceTempView(s"summary_input_$id")

    val keyString = keys(df).mkString(", ")
    val summaryExpressions = buildSummaryExpressions(df, summaryInputDf)
    val mapQuery =
      s"""SELECT
         |  ${summaryExpressions.map(_.inputAlias).mkString(",\n  ")},
         |  $keyString
         |FROM $summaryInputViewName""".stripMargin

    val mappedDf = tu.sql(mapQuery)
    val mappedViewName = s"mapped_input_$id"
    mappedDf.createOrReplaceTempView(mappedViewName)

    df.sparkSession.udf.register("count_histogram", udaf(HistogramAggregator))
    df.sparkSession.udf.register("array_histogram", udaf(ArrayStringHistogramAggregator))

    val aggQuery =
      s"""SELECT
         |  $keyString,
         |  ${summaryExpressions.map(_.aggAlias).mkString(",\n  ")}
         |FROM $mappedViewName
         |GROUP BY $keyString""".stripMargin

    val aggDf = tu.sql(aggQuery)
    logger.info(s"Schema of aggregated columns:\n${aggDf.schema}".green)
    aggDf -> summaryExpressions
  }
}

class SummaryPacker(confPath: String,
                    summaryExpressions: Seq[SummaryExpression],
                    tileSize: Window,
                    sliceColumns: Option[Seq[String]])(implicit tu: TableUtils)
    extends Serializable {
  // converts the flat output of computeSummaryDf into a pivot table
  // input schema: tile, slice, ds, col1_metric1, col1_metric2, col3_metric3 ...
  // output schema: ds, timestamp: Long, keyBytes: TileKey(tile, slice, column), valueBytes: TileSummaries,
  def packSummaryDf(df: DataFrame): (DataFrame, Unit) = {
    def indexOf(s: String): Int = df.columns.indexOf(s)

    val sliceColumn =
      sliceColumns.flatMap(_.headOption) // assuming 0 or 1 slice columns, more than 1 is not yet supported
    val sliceIndex = sliceColumn.map(indexOf).getOrElse(-1)

    val keyBuilder = { (column: String, row: sql.Row) =>
      val tileKey = new TileSummaryKey()
      tileKey.setName(confPath)
      tileKey.setSizeMillis(tileSize.millis)
      if (sliceIndex >= 0) tileKey.setSlice(row.getString(sliceIndex))
      tileKey.setColumn(column)
      tileKey
    }

    val func: sql.Row => Seq[TileRow] =
      Expressions.summaryPopulatorFunc(summaryExpressions, df.schema, keyBuilder, tu.partitionColumn)

    val packedSchema: types.StructType = types.StructType(
      Seq(
        types.StructField(tu.partitionColumn, types.StringType, nullable = false),
        types.StructField("timestamp", types.LongType, nullable = false),
        types.StructField("keyBytes", types.BinaryType, nullable = false),
        types.StructField("valueBytes", types.BinaryType, nullable = false)
      ))
    val rowEncoder = ExpressionEncoder(packedSchema)

    val packedDf = df
      .flatMap { row =>
        func(row).map { tileRow =>
          val serializer = compactSerializer.get()
          val keyBytes = serializer.serialize(tileRow.key)
          val valueBytes = serializer.serialize(tileRow.summaries)
          Row(tileRow.partition, tileRow.tileTs, keyBytes, valueBytes): Row
        }
      }(rowEncoder)
      .toDF()
    packedDf -> ()
  }
}

object Summarizer {
  // Initialize the logger
  private val logger = LoggerFactory.getLogger(getClass)

  def compute(api: Api,
              metadata: MetaData,
              ds: String,
              useLogs: Boolean = false,
              tileSize: Window = new Window(30, TimeUnit.MINUTES))(implicit tu: TableUtils): Unit = {
    // output of the actual job node - nodes could be join, groupBy, stagingQuery or model
    // will be the input to the summarization logic and drift computation logic

    val inputTable = if (useLogs) metadata.loggedTable else metadata.outputTable
    logger.info("Reading from table: " + metadata.loggedTable.yellow)

    val summaryTable = metadata.summaryTable
    val partitionFiller =
      new PartitionRunner(
        verb = "summarize",
        endDs = ds,
        inputTable = inputTable,
        outputTable = summaryTable,
        computeFunc = new Summarizer(api, metadata.name, tileSize = tileSize).computeSummaryDf
      )
    val exprs = partitionFiller.runInSequence

    val packedPartitionFiller =
      new PartitionRunner(
        verb = "pack_summaries",
        endDs = ds,
        inputTable = summaryTable,
        outputTable = metadata.packedSummaryTable,
        computeFunc = new SummaryPacker(
          metadata.getName,
          exprs.get,
          tileSize = tileSize,
          sliceColumns = None
        ).packSummaryDf
      )
    packedPartitionFiller.runInSequence
  }
}
