package ai.chronon.api.planner

import ai.chronon.api.{DataModel, GroupBy, PartitionSpec, Source, TableDependency, TableInfo, ThriftJsonCodec}
import ai.chronon.api.Extensions._
import ai.chronon.planner.{
  ConfPlan,
  GroupByBackfillNode,
  GroupByUploadNode,
  GroupByUploadToKVNode,
  GroupByStreamingNode,
  Node
}
import scala.collection.JavaConverters._

case class GroupByPlanner(groupBy: GroupBy)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[GroupBy](groupBy)(outputPartitionSpec) {

  private val confOutputPartitionSpec: PartitionSpec =
    groupBy.partitionSpec(outputPartitionSpec)

  private def validatePartitionIntervals(): Unit = {
    if (groupBy.dataModel == DataModel.ENTITIES && groupBy.inferredAccuracy == ai.chronon.api.Accuracy.SNAPSHOT) {
      PartitionSpecResolver.warnSubDailyEntitySnapshot(s"groupBy ${groupBy.metaData.name}", confOutputPartitionSpec)
    }
    for {
      sources <- Option(groupBy.sources).toSeq
      source <- sources.asScala
      query <- Option(source.query)
    } {
      PartitionSpecResolver.validateQueryGrid(
        groupBy.metaData.name,
        confOutputPartitionSpec,
        query,
        s"source ${source.rawTable}",
        source.dataModel,
        PartitionSpecResolver.sourceSpec(source, confOutputPartitionSpec)
      )
    }
    PartitionSpecResolver.validateEmbeddedJoinSourceTrees(
      Option(groupBy.sources).map(_.asScala.toSeq).getOrElse(Seq.empty),
      confOutputPartitionSpec
    )
  }

  // execInfo can be heavy - and we don't want to duplicate it
  private def eraseExecutionInfo: GroupBy = {
    val result = groupBy.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private val groupByTableDeps: Seq[TableDependency] = TableDependencies.fromGroupBy(groupBy)

  // upload nodes must also wait for the keyFilter's snapshot partition - without this dep the
  // upload launches before the filter lands and hard-fails at runtime. Deps live in executionInfo,
  // which is excluded from semantic hashing, so this cannot change any node's semantic hash.
  private val keyFilterDeps: Seq[TableDependency] =
    Option(groupBy.keyFilter).toSeq.flatMap { entityFilter =>
      val filterSource = new Source()
      filterSource.setEntities(entityFilter)
      TableDependencies.fromSource(filterSource)
    }

  def backfillNode: Node = {
    val defaultStepDays = if (groupBy.dataModel == DataModel.EVENTS) 15 else 1
    val effectiveStepDays =
      Option(groupBy.metaData.executionInfo).filter(_.isSetStepDays).map(_.stepDays).getOrElse(defaultStepDays)

    val metaData = MetaDataUtils.layer(groupBy.metaData,
                                       "group_by",
                                       groupBy.metaData.name + "__group_by",
                                       groupByTableDeps,
                                       Option(effectiveStepDays),
                                       Some(groupBy.metaData.outputTable))(confOutputPartitionSpec)

    val node = new GroupByBackfillNode().setGroupBy(eraseExecutionInfo)

    toGroupByNode(metaData, _.setGroupByBackfill(node))
  }

  private def semanticGroupBy(groupBy: GroupBy): GroupBy = {
    val semanticGroupBy = groupBy.deepCopy()
    semanticGroupBy.unsetMetaData()
    // Topics themselves don't affect the sem hash, we yank them out to allow users to change / fix misconfigured topics
    // without it requiring them to run the full chain of jobs
    Option(semanticGroupBy.sources).foreach { sources =>
      sources.forEach { source =>
        if (source.isSetEvents) source.getEvents.unsetTopic()
        else if (source.isSetEntities) source.getEntities.unsetMutationTopic()
      }
    }
    // keyFilter only shrinks batch uploads (which run daily anyway) - setting or changing it
    // should never invalidate backfills or re-trigger the deploy chain
    semanticGroupBy.unsetKeyFiltersRecursively()
    semanticGroupBy
  }

  private lazy val hashableGroupBy: GroupBy = semanticGroupBy(groupBy)
  private lazy val naturalSemanticHashBase: String = ThriftJsonCodec.hexDigest(hashableGroupBy)
  private lazy val semanticHashBaseOverride: Option[String] =
    GroupByPlanner.semanticHashBaseOverride(groupBy, naturalSemanticHashBase)

  private def toGroupByNode(metaData: ai.chronon.api.MetaData,
                            contentSetter: ai.chronon.planner.NodeContent => Unit): Node = {
    val node = toNode(metaData, contentSetter, hashableGroupBy)
    semanticHashBaseOverride.foreach(replacementBase => node.setSemanticHash(metaData.mixGridToken(replacementBase)))
    node
  }

  def uploadNode: Node = {
    val stepDays = 1 // GBUs write out data per day
    val metaData =
      MetaDataUtils.layer(groupBy.metaData,
                          "upload",
                          groupBy.metaData.name + "__upload",
                          groupByTableDeps ++ keyFilterDeps,
                          Some(stepDays))(confOutputPartitionSpec)

    val node = new GroupByUploadNode().setGroupBy(eraseExecutionInfo)
    toGroupByNode(metaData, _.setGroupByUpload(node))
  }

  def uploadToKVNode: Node = {
    val tableDep = new TableDependency()
      .setTableInfo(
        new TableInfo().setTable(uploadNode.metaData.outputTable).withSpec(confOutputPartitionSpec)
      )
      .setStartOffset(WindowUtils.zero())
      .setEndOffset(WindowUtils.zero())
    val uploadToKVTableDeps = Seq(tableDep)

    val metaData =
      MetaDataUtils.layer(
        groupBy.metaData,
        // Mode config uses the CLI run-mode spelling; the planner node identity below remains uploadToKV.
        "upload-to-kv",
        groupBy.metaData.name + s"__${GroupByPlanner.UploadToKV}",
        uploadToKVTableDeps,
        None
      )(confOutputPartitionSpec)

    val node = new GroupByUploadToKVNode().setGroupBy(eraseExecutionInfo)
    toGroupByNode(metaData, _.setGroupByUploadToKV(node))
  }

  def streamingNode: Option[Node] = {
    groupBy.streamingSource.map { _ =>
      // Streaming node has table dependency on the upload to KV
      val uploadToKVDep = new TableDependency()
        .setTableInfo(
          new TableInfo().setTable(uploadToKVNode.metaData.outputTable).withSpec(confOutputPartitionSpec)
        )
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())

      // If this GroupBy has a JoinSource, add dependency on upstream join's metadata upload
      val joinSourceDeps = TableDependencies.fromJoinSources(groupBy.sources)

      val streamingTableDeps = Seq(uploadToKVDep) ++ joinSourceDeps

      val metaData =
        MetaDataUtils.layer(
          groupBy.metaData,
          GroupByPlanner.Streaming,
          groupBy.metaData.name + s"__${GroupByPlanner.Streaming}",
          streamingTableDeps,
          None
        )(confOutputPartitionSpec)

      val node = new GroupByStreamingNode().setGroupBy(eraseExecutionInfo)
      toGroupByNode(metaData, _.setGroupByStreaming(node))
    }
  }

  override def buildPlan: ConfPlan = {
    validatePartitionIntervals()
    val backfill = backfillNode
    val sensorNodes = ExternalSourceSensorUtil
      .sensorNodes(backfill.metaData)(confOutputPartitionSpec)
      .map { es =>
        toNode(es.metaData, _.setExternalSourceSensor(es), ExternalSourceSensorUtil.semanticExternalSourceSensor(es))
      }
    val allNodes = Seq(backfill, uploadNode, uploadToKVNode) ++ sensorNodes ++ streamingNode.toSeq

    val deployTerminalNode = streamingNode.map(_.metaData.name).getOrElse(uploadToKVNode.metaData.name)

    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> backfill.metaData.name,
      ai.chronon.planner.Mode.DEPLOY -> deployTerminalNode
    )

    val confPlan = new ConfPlan()
      .setNodes(allNodes.asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
    confPlan
  }
}

object GroupByPlanner {
  val Streaming = "streaming"
  val UploadToKV = "uploadToKV"
  val SemanticHashBaseOverride = "spark.chronon.planner.group_by.semantic_hash_base_override"

  private val SemanticHashBaseOverridePattern = "^([^=]+)=([0-9a-f]{6})->([0-9a-f]{6})$".r
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private def semanticHashBaseOverride(groupBy: GroupBy, naturalBase: String): Option[String] = {
    val config = for {
      metaData <- Option(groupBy.metaData)
      executionInfo <- Option(metaData.executionInfo)
      conf <- Option(executionInfo.conf)
    } yield conf

    config.flatMap { conf =>
      val commonValue = Option(conf.common)
        .filter(_.containsKey(SemanticHashBaseOverride))
        .map(_.get(SemanticHashBaseOverride))
      val modeValues = Option(conf.modeConfigs)
        .toSeq
        .flatMap(_.asScala.toSeq)
        .flatMap { case (mode, values) =>
          Option(values)
            .filter(_.containsKey(SemanticHashBaseOverride))
            .map(mode -> _.get(SemanticHashBaseOverride))
        }

      require(
        commonValue.nonEmpty || modeValues.isEmpty,
        s"$SemanticHashBaseOverride must be set in common config for GroupBy ${groupBy.metaData.name}; mode-only overrides are not allowed"
      )

      commonValue.foreach { common =>
        val conflictingModes = modeValues.collect { case (mode, value) if value != common => mode }.sorted
        require(
          conflictingModes.isEmpty,
          s"$SemanticHashBaseOverride must have the same value in common config and all mode configs for GroupBy ${groupBy.metaData.name}; conflicting modes: ${conflictingModes.mkString(", ")}"
        )
      }

      commonValue.map {
        case SemanticHashBaseOverridePattern(configuredName, expectedBase, replacementBase) =>
          require(
            configuredName == groupBy.metaData.name,
            s"$SemanticHashBaseOverride targets GroupBy $configuredName but is configured on ${groupBy.metaData.name}"
          )
          require(
            expectedBase == naturalBase,
            s"$SemanticHashBaseOverride expected natural base $expectedBase for GroupBy $configuredName, but planner computed $naturalBase"
          )
          logger.warn(
            s"Applying guarded semantic hash base override for GroupBy $configuredName: $naturalBase -> $replacementBase"
          )
          replacementBase
        case value =>
          throw new IllegalArgumentException(
            s"Invalid $SemanticHashBaseOverride value '$value'; expected <fully-qualified-versioned-name>=<expected-natural-6hex>-><replacement-6hex>"
          )
      }
    }
  }
}
