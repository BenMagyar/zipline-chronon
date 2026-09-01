package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.runtimeMirror

/** Dynamically provide the read table format depending on table name.
  * This supports reading tables with heterogeneous formats.
  * This approach enables users to override and specify a custom format provider if needed. This is useful in
  * cases such as leveraging different library versions from what we support in the Chronon project (e.g. newer delta lake)
  * as well as working with custom internal company logic / checks.
  */
trait FormatProvider extends Serializable {

  def sparkSession: SparkSession

  def readFormat(tableName: String): Option[Format]
}

object FormatProvider {

  def from(session: SparkSession): FormatProvider =
    try {

      val clazzName =
        session.conf.get("spark.chronon.table.format_provider.class", classOf[DefaultFormatProvider].getName)

      val mirror = runtimeMirror(getClass.getClassLoader)
      val classSymbol = mirror.staticClass(clazzName)
      val classMirror = mirror.reflectClass(classSymbol)

      val constructor = classSymbol.primaryConstructor.asMethod
      val constructorMirror = classMirror.reflectConstructor(constructor)

      val reflected = constructorMirror(session)
      reflected.asInstanceOf[FormatProvider]

    } catch {

      case e: Exception =>
        throw new IllegalArgumentException(
          s"Failed to instantiate format provider. Please ensure the class is available in the classpath. Error: ${e.getMessage}",
          e
        )
    }

}
