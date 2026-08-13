package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Constants.{ContinuationKey, ListEntityType, ListLimit, MetadataDataset}
import ai.chronon.api.Extensions.{GroupByOps, StringOps, WindowOps, WindowUtils}
import ai.chronon.api.{GroupBy, MetaData, PartitionSpec, TilingUtils}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, ListRequest, ListResponse, ListValue}
import ai.chronon.online.metrics.Metrics
import com.google.api.core.ApiFuture
import com.google.cloud.RetryOption
import com.google.cloud.bigquery._
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.{CreateTableRequest, GCRules}
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.Range.{ByteStringRange, TimestampRange}
import com.google.cloud.bigtable.data.v2.models.{Filters, Query, RowMutation, TableId => BTTableId}
import com.google.protobuf.ByteString
import org.slf4j.{Logger, LoggerFactory}
import org.threeten.bp.Duration

import java.nio.charset.Charset
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.compat.java8.FutureConverters
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/** BigTable based KV store implementation. We store a few kinds of data in our KV store:
  * 1) Entity data - An example is thrift serialized Groupby / Join configs. If entities are updated / rewritten, we
  * serve the latest version.
  * 2) Timeseries data - This is either our batch IRs or streaming tiles for feature fetching.
  *
  * We have multi use-case tables for the _BATCH and _STREAMING time series tile data.
  * To ensure that data from different groupBys are isolated from each other, we prefix the key with the dataset name:
  * Row key: dataset#key
  *
  * In case of time series data that is likely to see many data points per day (e.g. streaming tiles), we
  * bucket the data by day to ensure that we don't need to filter a Row with thousands of cells (and also worry about the per Row size / cell count limits).
  * This also helps as GC in BigTable can take ~1 week. Without this day based bucketing we might have cells spanning a week.
  *
  * This row key structure looks like (tile size included in case of streaming tiles to support tile layering):
  * Row key: dataset#key#timestamp_rounded_to_day[#tileSize]
  *
  * Values are written to individual cells with timestamp of the time series point being the cell timestamp.
  *
  * Tables created via this client have a default TTL of 5 days and a max cell count of 10k. This is to ensure we don't
  * store data indefinitely and also to cap the amount of data we store.
  */
class BigTableKVStoreImpl(dataClient: BigtableDataClient,
                          adminClient: BigtableTableAdminClient,
                          maybeBigQueryClient: scala.Option[BigQuery] = None,
                          conf: Map[String, String] = Map.empty)
    extends KVStore {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import BigTableKVStore._

  // We keep data around for a 5 day TTL. This gives us a little buffer in case of incidents while still capping our storage
  private val DataTTL = Duration.ofDays(5)

  // Cap the maximum number of cells we store.
  private val MaxCellCount = 10000

  // BT docs (https://cloud.google.com/bigtable/docs/garbage-collection) cover this more - union ensures we GC data if either rule is met
  private val DefaultGcRules =
    GCRules.GCRULES.union().rule(GCRules.GCRULES.maxAge(DataTTL)).rule(GCRules.GCRULES.maxVersions(MaxCellCount))

  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("bigtable")

  protected val tableToContext = new TrieMap[String, Metrics.Context]()

  private def detectDatasetType(dataset: String, keyBytes: Array[Byte]): DatasetType = {
    // TODO: Add detection for MetricsDataset and EnhancedStatsDataset in future PRs
    FeaturesDataset
  }

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    val tableName = physicalTableName(dataset)
    try {
      if (!adminClient.exists(tableName)) {
        // we can explore split points if we need custom tablet partitioning. For now though, we leave this to BT
        val createTableRequest = CreateTableRequest.of(tableName).addFamily(ColumnFamilyString, DefaultGcRules)
        val table = adminClient.createTable(createTableRequest)
        // TODO: this actually submits an async task. thus, the submission can succeed but the task can fail.
        //  doesn't return a future but maybe we can poll
        logger.info(s"Created table: $table")
        metricsContext.increment("create.successes")
      } else {
        logger.info(s"Table $tableName already exists")
      }
    } catch {
      case e: Exception =>
        logger.error("Error creating table", e)
        metricsContext.increment("create.failures", Map("exception" -> e.getClass.getName))
    }
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    logger.debug(s"Performing multi-get for ${requests.size} requests")

    val requestsByType = requests.groupBy { req =>
      detectDatasetType(req.dataset, req.keyBytes)
    }

    val futures = requestsByType.map { case (FeaturesDataset, reqs) =>
      multiGetFeatures(reqs)
    // TODO: Add case (MetricsDataset, reqs) => multiGetMetrics(reqs)
    // TODO: Add case (EnhancedStatsDataset, reqs) => multiGetEnhancedStats(reqs)
    }.toSeq

    Future.sequence(futures).map(_.flatten)
  }

  private def multiGetFeatures(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    // Group requests by dataset and time range to avoid filter conflicts
    // For time-series data, we need separate queries for different time ranges
    val requestGroups = requests.groupBy { req =>
      val tableType = getTableType(req.dataset)
      tableType match {
        case StreamingTable =>
          // Group by dataset and time range for time-series data
          (req.dataset, req.startTsMillis, req.endTsMillis)
        case _ =>
          // Group only by dataset for non-time-series data
          (req.dataset, None, None)
      }
    }

    // Process each group separately
    val groupFutures = requestGroups.map { case ((dataset, startTs, endTs), groupRequests) =>
      readRowsMultiGet(dataset, groupRequests, startTs, endTs)
    }.toSeq

    // Combine results from all groups
    Future.sequence(groupFutures).map(_.flatten)
  }

  private def readRowsMultiGet(dataset: String,
                               requests: Seq[KVStore.GetRequest],
                               startTsMillis: scala.Option[Long],
                               endTsMillis: scala.Option[Long]): Future[Seq[KVStore.GetResponse]] = {
    val targetId = mapDatasetToTable(dataset)
    val datasetMetricsContext = tableToContext.getOrElseUpdate(
      targetId.toString,
      metricsContext.copy(dataset = targetId.toString)
    )
    val tableType = getTableType(dataset)

    // Calculate endTime once for use throughout the method
    val endTime = endTsMillis.getOrElse(System.currentTimeMillis())
    // Create query for this group (all requests now have the same time range)
    val query = Query.create(targetId)

    // Build the filter chain based on whether this is time-series data
    val filterChain = if (startTsMillis.isDefined) {
      // For time-series data, chain column family, qualifier, and timestamp filters
      Filters.FILTERS
        .chain()
        .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))
        .filter(Filters.FILTERS.qualifier().exactMatch(ColumnFamilyQualifierString))
        .filter(
          Filters.FILTERS
            .timestamp()
            .range()
            .startClosed(startTsMillis.get * 1000)
            .endClosed(endTime * 1000))
    } else {
      // For non-time-series data, chain column family, qualifier, and cellsPerRow limit filters
      Filters.FILTERS
        .chain()
        .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))
        .filter(Filters.FILTERS.qualifier().exactMatch(ColumnFamilyQualifierString))
        .filter(Filters.FILTERS.limit().cellsPerRow(1))
    }

    // Apply the chained filter to the query
    query.filter(filterChain)

    // Generate row keys for all requests
    val requestsWithRowKeys = requests.map { request =>
      val rowKeys: Seq[ByteString] = (request.startTsMillis, tableType) match {
        case (Some(startTs), StreamingTable) =>
          val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
          val tileSizeMs = tileKey.tileSizeMillis
          val baseKeyBytes = tileKey.keyBytes.asScala.map(_.toByte).toSeq
          generateTimeSeriesRowKeys(startTs, endTime, baseKeyBytes, dataset, Some(tileSizeMs))

        case _ =>
          // For non-timeseries data, just generate the single row key
          val baseRowKey = buildRowKey(request.keyBytes, dataset)
          Seq(ByteString.copyFrom(baseRowKey))
      }

      (request, rowKeys)
    }

    // Add all row keys to the query
    val allRowKeys = requestsWithRowKeys.flatMap(_._2)
    allRowKeys.foreach(query.rowKey)

    val startTs = System.currentTimeMillis()

    // Make BigTable call
    val apiFuture = dataClient.readRowsCallable().all().futureCall(query)
    val scalaResultFuture = googleFutureToScalaFuture(apiFuture)

    // Process results
    scalaResultFuture
      .map { rows =>
        datasetMetricsContext.distribution("multiGet.latency", System.currentTimeMillis() - startTs)
        datasetMetricsContext.increment("multiGet.successes")

        // Create a map for quick lookup by row key
        val rowKeyToRowMap = rows.asScala.map(row => row.getKey() -> row).toMap

        // Map back to original requests
        requestsWithRowKeys.map { case (request, rowKeys) =>
          // Get all cells from all row keys for this request
          val timedValues = rowKeys.flatMap { rowKey =>
            rowKeyToRowMap.get(rowKey).toSeq.flatMap { row =>
              row.getCells(ColumnFamilyString, ColumnFamilyQualifier).asScala.map { cell =>
                KVStore.TimedValue(cell.getValue.toByteArray, cell.getTimestamp / 1000)
              }
            }
          }.toSeq

          KVStore.GetResponse(request, Success(timedValues.toSeq))
        }
      }
      .recover { case e: Exception =>
        logger.error("Error getting values", e)
        datasetMetricsContext.increment("multiGet.bigtable_errors", Map("exception" -> e.getClass.getName))
        // If the batch fails, return failures for all requests in the batch
        requests.map { request =>
          KVStore.GetResponse(request, Failure(e))
        }
      }
  }

  // Helper method to generate row keys for time-series data
  // Note: This does NOT add keys to the query - the caller must do that after collecting all keys
  private def generateTimeSeriesRowKeys(startTs: Long,
                                        endTs: Long,
                                        keyBytes: Seq[Byte],
                                        dataset: String,
                                        maybeTileSize: scala.Option[Long] = None): Seq[ByteString] = {
    // we need to generate a rowkey corresponding to each day from the startTs to endTs
    val millisPerDay = 1.day.toMillis

    val startDay = startTs - (startTs % millisPerDay)
    val endDay = endTs - (endTs % millisPerDay)
    // get the rowKeys
    (startDay to endDay by millisPerDay)
      .map(dayTs => {
        val rowKey =
          maybeTileSize
            .map(tileSize => buildTiledRowKey(keyBytes, dataset, dayTs, tileSize))
            .getOrElse(buildRowKey(keyBytes, dataset, Some(dayTs)))
        ByteString.copyFrom(rowKey)
      })
      .toSeq
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    logger.debug(s"Performing list for ${request.dataset}")

    val listLimit = request.props.get(ListLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => defaultListLimit
    }

    val maybeListEntityType = request.props.get(ListEntityType)
    val maybeStartKey = request.props.get(ContinuationKey)

    val targetId = mapDatasetToTable(request.dataset)
    val datasetMetricsContext = tableToContext.getOrElseUpdate(
      targetId.toString,
      metricsContext.copy(dataset = targetId.toString)
    )

    // Build chained filter for list operations
    val chainedFilter = Filters.FILTERS
      .chain()
      .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))
      .filter(Filters.FILTERS.qualifier().exactMatch(ColumnFamilyQualifierString))
      // we also limit to the latest cell per row as we don't want clients to iterate over all prior edits
      .filter(Filters.FILTERS.limit().cellsPerRow(1))

    val query = Query
      .create(targetId)
      .filter(chainedFilter)
      .limit(listLimit)

    (maybeStartKey, maybeListEntityType) match {
      case (Some(startKey), _) =>
        // we have a start key, we use that to pick up from where we left off
        query.range(ByteStringRange.unbounded().startOpen(ByteString.copyFrom(startKey.asInstanceOf[Array[Byte]])))
      case (None, Some(listEntityType)) =>
        val startRowKey = buildRowKey(s"$listEntityType/".getBytes(Charset.forName("UTF-8")), request.dataset)
        query.range(ByteStringRange.unbounded().startOpen(ByteString.copyFrom(startRowKey)))
      case _ =>
        logger.debug("No start key or list entity type provided. Starting from the beginning")
    }

    val startTs = System.currentTimeMillis()
    val rowsApiFuture = dataClient.readRowsCallable().all.futureCall(query)
    val rowsScalaFuture = googleFutureToScalaFuture(rowsApiFuture)

    rowsScalaFuture
      .map { rows =>
        datasetMetricsContext.distribution("list.latency", System.currentTimeMillis() - startTs)
        datasetMetricsContext.increment("list.successes")

        val listValues = rows.asScala.flatMap { row =>
          row.getCells(ColumnFamilyString, ColumnFamilyQualifier).asScala.map { cell =>
            ListValue(row.getKey.toByteArray, cell.getValue.toByteArray)
          }
        }.toSeq

        val propsMap: Map[String, Any] =
          if (listValues.size < listLimit) {
            Map.empty // last page, we're done
          } else
            Map(ContinuationKey -> listValues.last.keyBytes)

        ListResponse(request, Success(listValues), propsMap)

      }
      .recover { case e: Exception =>
        logger.error("Error listing values", e)
        datasetMetricsContext.increment("list.bigtable_errors", Map("exception" -> e.getClass.getName))

        ListResponse(request, Failure(e), Map.empty)

      }
  }

  // We stick to individual put calls here as our invocations are fairly small sized sequences (couple of elements).
  // Using the individual mutate calls allows us to easily return fine-grained success/failure information in the form
  // our callers expect.
  override def multiPut(requests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.debug(s"Performing multi-put for ${requests.size} requests")

    val requestsByType = requests.groupBy { req =>
      detectDatasetType(req.dataset, req.keyBytes)
    }

    val futures = requestsByType.map { case (FeaturesDataset, reqs) =>
      multiPutFeatures(reqs)
    // TODO: Add case (MetricsDataset, reqs) => multiPutMetrics(reqs)
    // TODO: Add case (EnhancedStatsDataset, reqs) => multiPutEnhancedStats(reqs)
    }.toSeq

    Future.sequence(futures).map(_.flatten)
  }

  private def multiPutFeatures(requests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    val resultFutures = {
      requests.map { request =>
        val tableId = mapDatasetToTable(request.dataset)
        val datasetMetricsContext = tableToContext.getOrElseUpdate(
          tableId.toString,
          metricsContext.copy(dataset = tableId.toString)
        )

        val tableType = getTableType(request.dataset)
        val timestampInPutRequest = request.tsMillis.getOrElse(System.currentTimeMillis())

        val (rowKey, timestamp) = (request.tsMillis, tableType) match {
          case (Some(_), StreamingTable) =>
            val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
            val baseKeyBytes = tileKey.keyBytes.asScala.map(_.toByte).toSeq
            (buildTiledRowKey(baseKeyBytes, request.dataset, tileKey.tileStartTimestampMillis, tileKey.tileSizeMillis),
             tileKey.tileStartTimestampMillis)
          case _ =>
            (buildRowKey(request.keyBytes, request.dataset), timestampInPutRequest)
        }

        val timestampMicros = timestamp * 1000
        val mutation = RowMutation.create(tableId, ByteString.copyFrom(rowKey))
        val cellValue = ByteString.copyFrom(request.valueBytes)
        // if we have prior cells with the same timestamp, we queue up a delete operation before the put
        mutation.deleteCells(ColumnFamilyString,
                             ColumnFamilyQualifier,
                             TimestampRange.create(timestampMicros, timestampMicros + 1000))
        mutation.setCell(ColumnFamilyString, ColumnFamilyQualifier, timestampMicros, cellValue)

        val startTs = System.currentTimeMillis()
        val mutateApiFuture = dataClient.mutateRowAsync(mutation)
        val scalaFuture = googleFutureToScalaFuture(mutateApiFuture)
        scalaFuture
          .map { _ =>
            datasetMetricsContext.distribution("multiPut.latency", System.currentTimeMillis() - startTs)
            datasetMetricsContext.increment("multiPut.successes")
            true
          }
          .recover { case e: Exception =>
            logger.error("Error putting data", e)
            datasetMetricsContext.increment("multiPut.failures", Map("exception" -> e.getClass.getName))
            false
          }
      }
    }
    Future.sequence(resultFutures)
  }

  private def multiDelete(deleteRequests: Seq[DeleteRequest]): Future[Seq[Boolean]] = {
    val resultFutures = deleteRequests.map { req =>
      val tableId = mapDatasetToTable(req.dataset)
      val datasetMetricsContext = tableToContext.getOrElseUpdate(
        tableId.toString,
        metricsContext.copy(dataset = tableId.toString)
      )

      val rowKey = buildRowKey(req.keyBytes, req.dataset)
      val mutation = RowMutation.create(tableId, ByteString.copyFrom(rowKey))
      mutation.deleteRow()

      val mutateApiFuture = dataClient.mutateRowAsync(mutation)
      val scalaFuture = googleFutureToScalaFuture(mutateApiFuture)
      scalaFuture
        .map { _ =>
          true
        }
        .recover { case e: Exception =>
          logger.error(s"Error deleting data ${new String(req.keyBytes)}", e)
          datasetMetricsContext.increment("multiDelete.failures", Map("exception" -> e.getClass.getName))
          false
        }
    }
    Future.sequence(resultFutures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val uploader = conf.getOrElse("UPLOADER", "bigquery")

    uploader match {
      case "spark"    => bulkPutFromSpark(sourceOfflineTable, destinationOnlineDataSet, partition)
      case "bigquery" => bulkPutFromBigQuery(sourceOfflineTable, destinationOnlineDataSet, partition)
      case other =>
        logger.error(s"Unsupported uploader: $other")
        metricsContext.increment("bulkPut.failures", Map("exception" -> "unsupported_uploader"))
        throw new IllegalArgumentException(s"Unsupported uploader: $other.")
    }
  }

  private def bulkPutFromSpark(sourceOfflineTable: String,
                               destinationOnlineDataSet: String,
                               partition: String): Unit = {
    val startTs = System.currentTimeMillis()

    logger.info(
      s"Triggering Spark-based bulk load for dataset: $destinationOnlineDataSet, " +
        s"table: $sourceOfflineTable, partition: $partition")

    try {
      // Use Spark2BigTableLoader to load data from Hive/Delta tables
      val loaderArgs = Array(
        "--table-name",
        sourceOfflineTable,
        "--dataset",
        destinationOnlineDataSet,
        "--end-ds",
        partition,
        "--project-id",
        adminClient.getProjectId,
        "--instance-id",
        adminClient.getInstanceId
      )

      // Run the Spark job
      Spark2BigTableLoader.main(loaderArgs)

      logger.info("Spark-based bulk load completed successfully")
      metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
      metricsContext.increment("bulkPut.successes")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to run Spark-based bulk load for $sourceOfflineTable", e)
        metricsContext.increment("bulkPut.failures", Map("exception" -> e.getClass.getName))
        throw e
    }
  }

  private def bulkPutFromBigQuery(sourceOfflineTable: String,
                                  destinationOnlineDataSet: String,
                                  partition: String): Unit = {
    if (maybeBigQueryClient.isEmpty) {
      logger.error("Need the BigQuery client available to export data to BigTable")
      metricsContext.increment("bulkPut.failures", Map("exception" -> "missinguploadclients"))
      throw new RuntimeException("BigQuery client is needed to export data to BigTable")
    }

    // we write groupby data to 1 large multi use-case table
    val batchTable = "GROUPBY_BATCH"

    // we use the endDs + span to indicate the timestamp of all the cell data we upload for endDs
    // this is used in the KV store multiget calls
    val partitionSpec = PartitionSpec("ds", "yyyy-MM-dd", WindowUtils.Day.millis)
    val endDsPlusOne = partitionSpec.epochMillis(partition) + partitionSpec.spanMillis

    // we need to sanitize and append the batch suffix to the groupBy name as that's
    // what we use to look things up while fetching
    val groupBy = new GroupBy().setMetaData(new MetaData().setName(destinationOnlineDataSet))
    val datasetName = groupBy.batchDataset

    val exportQuery =
      s"""
         |EXPORT DATA OPTIONS (
         |  format='CLOUD_BIGTABLE',
         |  overwrite=true,
         |  uri="https://bigtable.googleapis.com/projects/${adminClient.getProjectId}/instances/${adminClient.getInstanceId}/appProfiles/GROUPBY_INGEST/tables/$batchTable",
         |  bigtable_options='''{
         |   "columnFamilies" : [
         |      {
         |        "familyId": "cf",
         |        "encoding": "BINARY",
         |        "columns": [
         |           {"qualifierString": "value", "fieldName": ""}
         |        ]
         |      }
         |   ]
         |}'''
         |) AS
         |SELECT
         |  CONCAT(CAST(CONCAT('$datasetName', '#') AS BYTES), key_bytes) as rowkey,
         |  value_bytes as cf,
         |  TIMESTAMP_MILLIS($endDsPlusOne) as _CHANGE_TIMESTAMP
         |FROM $sourceOfflineTable
         |WHERE ds = '$partition'
         |""".stripMargin
    logger.info(s"Kicking off bulkLoad with query:\n$exportQuery")

    maybeBigQueryClient.foreach { bigQueryClient =>
      val queryConfig = QueryJobConfiguration
        .newBuilder(exportQuery)
        .build()

      val startTs = System.currentTimeMillis()
      // we append the timestamp to the jobID as BigQuery doesn't allow us to re-run the same job
      // Pin the job to GCP_LOCATION so it lands in the same region as the BQ reservation
      // assigned to the project. Without an explicit location, the BQ client won't specify any location
      val jobId = {
        GcpApiImpl
          .getOptional(GcpApiImpl.GcpLocation, conf)
          .fold({
            logger.warn(s"GCP_LOCATION is not set, starting job without specifying location.")
            JobId.of(adminClient.getProjectId,
                     s"export_${sourceOfflineTable.sanitize}_to_bigtable_${partition}_$startTs")
          })(bqLocation =>
            JobId
              .newBuilder()
              .setProject(adminClient.getProjectId)
              .setLocation(bqLocation)
              .setJob(s"export_${sourceOfflineTable.sanitize}_to_bigtable_${partition}_$startTs")
              .build())
      }
      val job: Job = bigQueryClient.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
      logger.info(s"Export job started with Id: $jobId and link: ${job.getSelfLink}")
      val retryConfig =
        BigQueryRetryConfig.newBuilder
          .retryOnMessage(BigQueryErrorMessages.RATE_LIMIT_EXCEEDED_MSG)
          .retryOnMessage(BigQueryErrorMessages.JOB_RATE_LIMIT_EXCEEDED_MSG)
          .build

      val initialRetryDelay = Duration.ofMinutes(1)
      val totalRetryTimeout = Duration.ofHours(6)
      logger.info(s"We will wait for $totalRetryTimeout for the job to complete")
      val completedJob = job.waitFor(retryConfig,
                                     RetryOption.initialRetryDelay(initialRetryDelay),
                                     RetryOption.totalTimeout(totalRetryTimeout))
      if (completedJob == null) {
        // job no longer exists
        logger.error(s"Job corresponding to $jobId no longer exists")
        metricsContext.increment("bulkPut.failures", Map("exception" -> "missingjob"))
        throw new RuntimeException(s"Export job corresponding to $jobId no longer exists")
      } else if (completedJob.getStatus.getError != null) {
        logger.error(s"Job failed with error: ${completedJob.getStatus.getError}")
        metricsContext.increment("bulkPut.failures",
                                 Map("exception" -> s"${completedJob.getStatus.getError.getReason}"))
        throw new RuntimeException(s"Export job failed with error: ${completedJob.getStatus.getError}")
      } else {
        logger.info("Export job completed successfully")
        metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
        metricsContext.increment("bulkPut.successes")
      }
    }
  }

  override def init(props: Map[String, Any]): Unit = {
    super.init(props)

    val warmupLengthMillis: Long =
      conf.get("WARMUP_TIMEOUT_MS").map(_.toLong).getOrElse(30000L)
    // Perform some dummy operations to warm up the client
    // This can help reduce latency for the first real operations.
    // Intentionally getting and deleting non-existent keys below to warm up.

    val testKey = "warmup_key"
    logger.info(s"Warming up KVStore with key prefix $testKey (timeout=${warmupLengthMillis}ms)")
    try {
      val getFutures = this.multiGet(
        (1 to 1000)
          .map(i =>
            GetRequest(
              keyBytes = s"${testKey}_$i".getBytes,
              dataset = MetadataDataset
            ))
          .toSeq
      )
      val deleteFutures = this.multiDelete(
        (1 to 1000)
          .map(i =>
            DeleteRequest(
              keyBytes = s"${testKey}_$i".getBytes,
              dataset = MetadataDataset
            ))
          .toSeq
      )
      Await.result(getFutures, warmupLengthMillis.milliseconds)
      Await.result(deleteFutures, warmupLengthMillis.milliseconds)
      logger.info("KVStore warm-up completed successfully")
    } catch {
      case e: Exception =>
        logger.warn(s"KVStore warm-up failed after ${warmupLengthMillis}ms", e)
    }
  }
}

object BigTableKVStore {

  // Default list limit
  val defaultListLimit: Int = 100

  case class DeleteRequest(keyBytes: Array[Byte], dataset: String)

  sealed trait TableType
  case object BatchTable extends TableType
  case object StreamingTable extends TableType

  sealed trait DatasetType {
    def ttl: Duration
    def maxCellVersions: Int
    def columnFamilies: Seq[String]
  }

  case object FeaturesDataset extends DatasetType {
    val ttl: Duration = Duration.ofDays(5)
    val maxCellVersions: Int = 10000
    val columnFamilies: Seq[String] = Seq("cf")
  }

  /** row key (with tiling) convention:
    * <dataset>#<entity-key>#<start_date>#<tile_size>
    *
    *  row key (without tiling) convention:
    *  <dataset>#<entity_key>#<start_date>
    */
  def buildTiledRowKey(baseKeyBytes: Seq[Byte], dataset: String, ts: Long, tileSizeMs: Long): Array[Byte] = {
    val baseRowKey = s"$dataset#".getBytes(Charset.forName("UTF-8")) ++ baseKeyBytes
    val dayTs = ts - (ts % 1.day.toMillis)
    baseRowKey ++ s"#$dayTs".getBytes(Charset.forName("UTF-8")) ++ s"#$tileSizeMs".getBytes(Charset.forName("UTF-8"))
  }

  // We prefix the dataset name to the key to ensure we can have multiple datasets in the same table
  def buildRowKey(baseKeyBytes: Seq[Byte], dataset: String, maybeTs: scala.Option[Long] = None): Array[Byte] = {
    val baseRowKey = s"$dataset#".getBytes(Charset.forName("UTF-8")) ++ baseKeyBytes
    maybeTs match {
      case Some(ts) =>
        // For time series data, we append the day timestamp to the row key to ensure that time series points across different
        // days are split across rows
        val dayTs = ts - (ts % 1.day.toMillis)
        baseRowKey ++ s"#$dayTs".getBytes(Charset.forName("UTF-8"))
      case _ => baseRowKey
    }
  }

  // Returns the physical table name string used by the admin client (exists/create)
  def physicalTableName(dataset: String): String = {
    if (dataset.endsWith("_BATCH")) "GROUPBY_BATCH"
    else if (dataset.endsWith("_STREAMING")) "GROUPBY_STREAMING"
    else dataset
  }

  def mapDatasetToTable(dataset: String): BTTableId = BTTableId.of(physicalTableName(dataset))

  def getTableType(dataset: String): TableType = {
    dataset match {
      case d if d.endsWith("_BATCH")     => BatchTable
      case d if d.endsWith("_STREAMING") => StreamingTable
      case _                             => BatchTable // default to batch table for tables like chronon_metadata
    }
  }

  def googleFutureToScalaFuture[T](apiFuture: ApiFuture[T]): Future[T] = {
    val completableFuture = ApiFutureUtils.toCompletableFuture(apiFuture)
    FutureConverters.toScala(completableFuture)
  }

  val ColumnFamilyString: String = "cf"
  val ColumnFamilyQualifierString: String = "value"
  val ColumnFamilyQualifier: ByteString = ByteString.copyFromUtf8(ColumnFamilyQualifierString)
}
