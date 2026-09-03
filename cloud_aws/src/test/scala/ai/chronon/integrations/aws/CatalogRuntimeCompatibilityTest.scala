package ai.chronon.integrations.aws

import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.scalatest.flatspec.AnyFlatSpec

class CatalogRuntimeCompatibilityTest extends AnyFlatSpec {

  it should "load the catalog implementations from the cloud AWS runtime" in {
    Seq(
      "io.unitycatalog.spark.UCSingleCatalog",
      "org.apache.iceberg.spark.SparkCatalog"
    ).foreach { className =>
      val catalog = Class.forName(className).getDeclaredConstructor().newInstance()
      assert(catalog.isInstanceOf[CatalogPlugin])
    }

    assert(Class.forName("io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider") != null)
  }
}
