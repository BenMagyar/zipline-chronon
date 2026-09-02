package ai.chronon.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions

/** Spark 4 parses WHEN NOT MATCHED BY SOURCE natively. Keep the extension class as a no-op so
  * existing spark.sql.extensions settings remain valid during the upgrade.
  */
class ChrononMergeExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = ()
}
