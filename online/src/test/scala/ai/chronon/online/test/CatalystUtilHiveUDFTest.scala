package ai.chronon.online.test

import ai.chronon.online.CatalystUtil
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec

class CatalystUtilHiveUDFTest extends AnyFlatSpec with CatalystUtilTestSparkSQLStructs {

  // Hive UDFs (subclass of org.apache.hadoop.hive.ql.exec.UDF) defined in ExampleUDFs.scala.
  // These are on the test classpath so no ADD JAR is needed.
  "catalyst util" should "register Hive UDFs via CREATE FUNCTION and evaluate them" in {
    val setups = Seq(
      "CREATE FUNCTION MINUS_TWO AS 'ai.chronon.online.test.Minus_Two'",
      "CREATE FUNCTION CAT_STR AS 'ai.chronon.online.test.Cat_Str'"
    )
    val selects = Seq(
      "a" -> "MINUS_TWO(int32_x)",
      "b" -> "CAT_STR(string_x)"
    )
    val cu = new CatalystUtil(CommonScalarsStruct, selects = selects, setups = setups)
    val resList = cu.performSql(CommonScalarsRow)
    assertEquals(resList.size, 1)
    val resMap = resList.head
    assertEquals(resMap.size, 2)
    assertEquals(resMap("a"), Int.MaxValue - 2)
    assertEquals(resMap("b"), "hello123")
  }
}
