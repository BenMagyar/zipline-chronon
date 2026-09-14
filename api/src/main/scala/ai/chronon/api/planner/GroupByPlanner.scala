package ai.chronon.api.planner

import ai.chronon.api.{DataModel, GroupBy, PartitionSpec, Source, TableDependency, TableInfo}
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

    toNode(metaData, _.setGroupByBackfill(node), semanticGroupByForBatch(groupBy))
  }

  private def semanticGroupBy(groupBy: GroupBy): GroupBy = {
    val semanticGroupBy = groupBy.deepCopy()
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

  /** Variant for batch nodes (backfill, upload, uploadToKV). Batch outputs are byte-identical
    * across online strategies — the streaming side reads/writes them differently but the
    * underlying batch artifacts don't change. Without this, flipping `onlineStrategy` (e.g.
    * DEFAULT ↔ STREAMING_MEGATILES) causes BatchNodeRunner to archive and re-run every batch
    * job downstream of the GroupBy for no observable change in output data.
    */
  private def semanticGroupByForBatch(groupBy: GroupBy): GroupBy = {
    val gb = semanticGroupBy(groupBy)
    gb.unsetOnlineStrategy()
    gb
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
    toNode(metaData, _.setGroupByUpload(node), semanticGroupByForBatch(groupBy))
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
        GroupByPlanner.UploadToKV,
        groupBy.metaData.name + s"__${GroupByPlanner.UploadToKV}",
        uploadToKVTableDeps,
        None
      )(confOutputPartitionSpec)

    val node = new GroupByUploadToKVNode().setGroupBy(eraseExecutionInfo)
    toNode(metaData, _.setGroupByUploadToKV(node), semanticGroupByForBatch(groupBy))
  }

  def streamingNode: Option[Node] = {
    groupBy.streamingSource.map { _ =>
      // Streaming node depends on whichever node actually produces the data (and, for PUSH,
      // the GroupByServingInfo) it needs before it can safely start. For PUSH, uploadToKVNode
      // is never scheduled (see buildPlan) and GroupByUpload publishes serving info directly
      // from uploadNode, so the dependency must point there instead of at uploadToKVNode.
      val groupByOps = new GroupByOps(groupBy)
      val upstreamNode = if (groupByOps.isPushEnabled) uploadNode else uploadToKVNode
      val uploadToKVDep = new TableDependency()
        .setTableInfo(
          new TableInfo().setTable(upstreamNode.metaData.outputTable).withSpec(confOutputPartitionSpec)
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
      toNode(metaData, _.setGroupByStreaming(node), semanticGroupBy(groupBy))
    }
  }

  override def buildPlan: ConfPlan = {
    validatePartitionIntervals()
    val backfill = backfillNode
    val sensorNodes = ExternalSourceSensorUtil
      .sensorNodes(backfill.metaData)(confOutputPartitionSpec)
      .map { es =>
        toNode(es.metaData, _.setExternalSourceSensor(es), es)
      }
    // PUSH GroupBys write serving info to KV directly from GroupByUpload — skip the
    // KVUploadNodeRunner step (which would bulkPut entity rows that Flink reads from Iceberg).
    val groupByOps = new GroupByOps(groupBy)
    val kvUploadNodes = if (groupByOps.isPushEnabled) Seq.empty else Seq(uploadToKVNode)
    val allNodes = Seq(backfill, uploadNode) ++ kvUploadNodes ++ sensorNodes ++ streamingNode.toSeq

    val deployTerminalNode = streamingNode
      .map(_.metaData.name)
      .getOrElse(if (kvUploadNodes.nonEmpty) uploadToKVNode.metaData.name else uploadNode.metaData.name)

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
}
