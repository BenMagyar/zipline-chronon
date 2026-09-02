package ai.chronon.spark.submission

import org.junit.Assert.{assertEquals, assertFalse}
import org.scalatest.flatspec.AnyFlatSpec

class FlinkMemoryConfigTest extends AnyFlatSpec {

  "flinkMemoryConfig" should "use caller supplied default tier when taskmanager memory is absent" in {
    val props = FlinkMemoryConfig.flinkMemoryConfig(Map.empty, FlinkMemoryConfig.TaskManager64G)

    assertEquals("64G", props("taskmanager.memory.process.size"))
    assertEquals("4", props("taskmanager.numberOfTaskSlots"))
    assertEquals("1G", props("taskmanager.memory.network.min"))
    assertEquals("2G", props("taskmanager.memory.network.max"))
    assertEquals("1G", props("taskmanager.memory.task.off-heap.size"))
  }

  it should "use small profile for sub-16G taskmanager memory" in {
    val props = FlinkMemoryConfig.flinkMemoryConfig(
      Map("taskmanager.memory.process.size" -> "4G"),
      FlinkMemoryConfig.TaskManager64G
    ) ++ Map("taskmanager.memory.process.size" -> "4G")

    assertEquals("4G", props("taskmanager.memory.process.size"))
    assertEquals("1", props("taskmanager.numberOfTaskSlots"))
    assertFalse(props.contains("taskmanager.memory.network.min"))
    assertFalse(props.contains("taskmanager.memory.network.max"))
    assertFalse(props.contains("taskmanager.memory.managed.fraction"))
    assertFalse(props.contains("taskmanager.memory.jvm-metaspace.size"))
    assertFalse(props.contains("taskmanager.memory.task.off-heap.size"))
  }

  it should "select tuned tiers for explicit 16G 32G and 64G taskmanager memory" in {
    val tier16 = FlinkMemoryConfig.flinkMemoryConfig(
      Map("taskmanager.memory.process.size" -> "16G"),
      FlinkMemoryConfig.TaskManager64G
    )
    val tier32 = FlinkMemoryConfig.flinkMemoryConfig(
      Map("taskmanager.memory.process.size" -> "32G"),
      FlinkMemoryConfig.TaskManager16G
    )
    val tier64 = FlinkMemoryConfig.flinkMemoryConfig(
      Map("taskmanager.memory.process.size" -> "64G"),
      FlinkMemoryConfig.TaskManager16G
    )

    assertEquals("1", tier16("taskmanager.numberOfTaskSlots"))
    assertEquals("256m", tier16("taskmanager.memory.network.min"))
    assertEquals("2", tier32("taskmanager.numberOfTaskSlots"))
    assertEquals("512m", tier32("taskmanager.memory.task.off-heap.size"))
    assertEquals("4", tier64("taskmanager.numberOfTaskSlots"))
    assertEquals("1G", tier64("taskmanager.memory.task.off-heap.size"))
  }
}
