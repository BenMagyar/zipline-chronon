package ai.chronon.spark.submission

object FlinkMemoryConfig {

  val TaskManagerProcessMemoryKey = "taskmanager.memory.process.size"

  // TM memory tiers. TM sizing follows the following conventions:
  // 64G: 4 slots, tuned network/managed/metaspace settings matching prior load testing on Dataproc
  // 32G: 2 slots, halved from 64G settings
  // 16G: 1 slot, tuned settings for a smaller footprint
  // Small (anything else, e.g. 4G/8G): 1 slot, no explicit memory tuning. Flink defaults apply,
  // and the container size is taken directly from jobProperties.
  sealed trait MemoryTier {
    // None means Flink defaults
    def tmProcessMemory: Option[String]
    def jmProcessMemory: String = "4G"
    def taskSlots: Int
    def networkMin: Option[String]
    def networkMax: Option[String]
    def managedFraction: Option[String]
    def metaspaceSize: Option[String]
    def taskOffHeap: Option[String]
  }

  case object TaskManager64G extends MemoryTier {
    val tmProcessMemory = Some("64G")
    val taskSlots = 4
    val networkMin = Some("1G")
    val networkMax = Some("2G")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("1G")
  }

  case object TaskManager32G extends MemoryTier {
    val tmProcessMemory = Some("32G")
    val taskSlots = 2
    val networkMin = Some("512m")
    val networkMax = Some("1G")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("512m")
  }

  case object TaskManager16G extends MemoryTier {
    val tmProcessMemory = Some("16G")
    val taskSlots = 1
    val networkMin = Some("256m")
    val networkMax = Some("512m")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("256m")
  }

  case object SmallTaskManager extends MemoryTier {
    val tmProcessMemory = None
    val taskSlots = 1
    val networkMin = None
    val networkMax = None
    val managedFraction = None
    val metaspaceSize = None
    val taskOffHeap = None
  }

  def parseTmMemoryTier(jobProperties: Map[String, String], defaultTier: MemoryTier): MemoryTier =
    jobProperties.get(TaskManagerProcessMemoryKey) match {
      case Some(v) if v.toUpperCase.startsWith("64") => TaskManager64G
      case Some(v) if v.toUpperCase.startsWith("32") => TaskManager32G
      case Some(v) if v.toUpperCase.startsWith("16") => TaskManager16G
      case None                                      => defaultTier
      case _                                         => SmallTaskManager
    }

  def flinkMemoryConfig(jobProperties: Map[String, String], defaultTier: MemoryTier): Map[String, String] = {
    val tier = parseTmMemoryTier(jobProperties, defaultTier)
    val base = Map(
      "jobmanager.memory.process.size" -> tier.jmProcessMemory,
      "taskmanager.numberOfTaskSlots" -> tier.taskSlots.toString
    )
    val optional = List(
      tier.tmProcessMemory.map(TaskManagerProcessMemoryKey -> _),
      tier.networkMin.map("taskmanager.memory.network.min" -> _),
      tier.networkMax.map("taskmanager.memory.network.max" -> _),
      tier.managedFraction.map("taskmanager.memory.managed.fraction" -> _),
      tier.metaspaceSize.map("taskmanager.memory.jvm-metaspace.size" -> _),
      tier.taskOffHeap.map("taskmanager.memory.task.off-heap.size" -> _)
    ).flatten.toMap
    base ++ optional
  }
}
