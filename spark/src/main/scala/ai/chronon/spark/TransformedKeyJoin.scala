package ai.chronon.spark

import ai.chronon.api.Extensions._
import ai.chronon.api.JoinPart
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}

// Handles joins where a GroupBy key is stored under a canonical transformed value, for example
// `keys = {"query": "stem(query)"}`. The right side already contains the transformed/canonical key
// because GroupBy applies key transforms before aggregation. The left side still contains the raw
// join key from the query, so the join must compute the same canonical value before joining.
//
// Key mappings make this subtle. If a join maps `leftKey -> rightKey`, transform expressions are
// authored against the GroupBy/right key name, while the left dataframe stores the raw value under
// `leftKey`. Rather than overwrite `rightKey` on the left dataframe, which can collide with an
// unrelated user column, we create an internal input alias for `leftKey` and rewrite the parsed
// Catalyst expression so references to `rightKey` point at that internal alias. Function calls and
// UDFs remain unchanged because only unresolved attribute nodes are rewritten. The actual join uses
// internal temp columns, and those internal columns are dropped after the coalesced join finishes.
private[spark] case class TransformedJoinKey(leftKey: String,
                                             rightKey: String,
                                             tempKey: String,
                                             inputKey: String,
                                             leftTransform: String)

private[spark] class TransformedKeyJoin(joinPart: JoinPart, occupiedColumns: Set[String]) {

  private val transforms = joinPart.groupBy.keyTransformsScala

  val keys: Seq[TransformedJoinKey] = {
    val (result, _) = joinPart.leftToRight.toSeq.foldLeft((Seq.empty[TransformedJoinKey], occupiedColumns)) {
      case ((acc, usedColumns), (leftKey, rightKey)) if transforms.contains(rightKey) =>
        val tempKey = internalColumn("key", leftKey, usedColumns)
        val inputKey = internalColumn("input", rightKey, usedColumns + tempKey)
        val leftTransform = rewriteTransform(transforms(rightKey), rightKey, inputKey)
        (acc :+ TransformedJoinKey(leftKey, rightKey, tempKey, inputKey, leftTransform),
         usedColumns ++ Seq(tempKey, inputKey))

      case ((acc, usedColumns), _) => (acc, usedColumns)
    }
    result
  }

  private def internalColumn(prefix: String, key: String, usedColumns: Set[String]): String = {
    val base = s"${prefix}_$key"
    Iterator
      .iterate(0)(_ + 1)
      .map {
        case 0 => base
        case i => s"${base}_$i"
      }
      .dropWhile(usedColumns.contains)
      .next()
  }

  private def rewriteTransform(transform: String, from: String, to: String): String =
    CatalystSqlParser
      .parseExpression(transform)
      .transform {
        case attr: UnresolvedAttribute if attr.nameParts.headOption.contains(from) =>
          UnresolvedAttribute(to +: attr.nameParts.tail)
      }
      .sql

  def isEmpty: Boolean = keys.isEmpty

  def prepareLeft(leftDf: DataFrame): DataFrame = {
    val withInputKeys = keys.foldLeft(leftDf) { case (df, key) =>
      df.withColumn(key.inputKey, col(key.leftKey))
    }

    keys.foldLeft(withInputKeys) { case (df, key) =>
      df.withColumn(key.tempKey, expr(key.leftTransform))
    }
  }

  def prepareRight(rightDf: DataFrame): DataFrame =
    keys.foldLeft(rightDf) { case (df, key) =>
      df.withColumn(key.tempKey, col(key.leftKey))
    }

  def joinKeys(baseKeys: Seq[String]): Seq[String] = {
    val leftToTempKey = keys.map(key => key.leftKey -> key.tempKey).toMap
    baseKeys.map(key => leftToTempKey.getOrElse(key, key))
  }

  def columnsToDropAfterJoin: Seq[String] =
    keys.flatMap(key => Seq(key.tempKey, key.inputKey))
}
