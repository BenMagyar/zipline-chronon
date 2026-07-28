package ai.chronon.integrations.redis

import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore.{GetRequest, PutRequest}

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration._

/** Deterministic Redis fixture matching a bulk search fetch after join-level context deduplication. */
private[redis] object RedisFetcherReadWorkload {

  sealed trait Ownership
  case object Context extends Ownership
  case object Candidate extends Ownership

  sealed trait ReadShape {
    def streamingPoints(config: Config): Int
  }
  case object Snapshot extends ReadShape {
    override def streamingPoints(config: Config): Int = 0
  }
  case object LastValue extends ReadShape {
    override def streamingPoints(config: Config): Int = 1
  }
  case object HourlyTiled extends ReadShape {
    override def streamingPoints(config: Config): Int = config.tiledPoints
  }

  final case class Config(
      candidates: Int = 50,
      contextGroupBys: Int = 25,
      candidateGroupBys: Int = 25,
      contextLastGroupBys: Int = 3,
      contextTiledGroupBys: Int = 2,
      candidateLastGroupBys: Int = 2,
      candidateTiledGroupBys: Int = 3,
      candidateEmbeddingGroupBys: Int = 2,
      tiledPoints: Int = 24,
      tileSizeMillis: Long = 1.hour.toMillis,
      scalarPayloadBytes: Int = 4,
      embeddingPayloadBytes: Int = 512 * java.lang.Float.BYTES
  ) {
    require(candidates > 0)
    require(contextGroupBys > 0 && candidateGroupBys > 0)
    require(contextLastGroupBys >= 0 && contextTiledGroupBys >= 0)
    require(candidateLastGroupBys >= 0 && candidateTiledGroupBys >= 0)
    require(contextLastGroupBys + contextTiledGroupBys <= contextGroupBys)
    require(candidateLastGroupBys + candidateTiledGroupBys <= candidateGroupBys)
    require(
      candidateEmbeddingGroupBys >= 0 &&
        candidateEmbeddingGroupBys <= candidateGroupBys - candidateLastGroupBys - candidateTiledGroupBys)
    require(tiledPoints > 1 && tileSizeMillis > 0)
    require(scalarPayloadBytes > 0 && embeddingPayloadBytes > 0)

    val contextSnapshotGroupBys: Int = contextGroupBys - contextLastGroupBys - contextTiledGroupBys
    val candidateSnapshotGroupBys: Int = candidateGroupBys - candidateLastGroupBys - candidateTiledGroupBys
    val totalGroupBys: Int = contextGroupBys + candidateGroupBys
    val totalStreamingGroupBys: Int =
      contextLastGroupBys + contextTiledGroupBys + candidateLastGroupBys + candidateTiledGroupBys
  }

  final case class GroupBySpec(
      name: String,
      ownership: Ownership,
      readShape: ReadShape,
      payloadBytes: Int
  ) {
    val batchDataset: String = s"${name}_BATCH"
    val streamingDataset: Option[String] = readShape match {
      case Snapshot => None
      case _        => Some(s"${name}_STREAMING")
    }
  }

  final case class ReadProfile(
      name: String,
      requests: Vector[GetRequest],
      expectedTimedValues: Long,
      expectedPayloadBytes: Long
  ) {
    val batchGetCommands: Int = requests.count(_.startTsMillis.isEmpty)
    val zrangeCommands: Int = requests.iterator
      .filter(_.startTsMillis.isDefined)
      .map { request =>
        val startTs = request.startTsMillis.get
        val endTs = request.endTsMillis.getOrElse(startTs)
        val startDay = startTs - (startTs % 1.day.toMillis)
        val endDay = endTs - (endTs % 1.day.toMillis)
        ((endDay - startDay) / 1.day.toMillis + 1L).toInt
      }
      .sum
    val redisCommands: Int = batchGetCommands + zrangeCommands
    val groupByRequests: Int = batchGetCommands
  }

  final case class Workload(
      config: Config,
      groupBys: Vector[GroupBySpec],
      datasets: Vector[String],
      puts: Vector[PutRequest],
      logical: ReadProfile,
      deduplicated: ReadProfile,
      deduplicatedCrossDay: ReadProfile,
      batchOnly: ReadProfile,
      batchCacheDemand: Vector[ReadProfile],
      batchCacheDemandCrossDay: Vector[ReadProfile],
      uniqueBatchRequests: Int,
      uniqueStreamingRequests: Int
  ) {
    val logicalGroupByRequests: Int = config.candidates * config.totalGroupBys
    val uniqueGroupByRequests: Int = uniqueBatchRequests

    def batchCacheDemandAt(hitPercent: Int): ReadProfile =
      batchCacheDemand.find(_.name == s"synthetic-context-hot-candidate-hit-$hitPercent").getOrElse {
        throw new IllegalArgumentException(s"No batch cache demand profile for $hitPercent%")
      }

    def batchCacheDemandCrossDayAt(hitPercent: Int): ReadProfile =
      batchCacheDemandCrossDay.find(_.name == s"synthetic-context-hot-candidate-hit-$hitPercent-cross-day-24h").getOrElse {
        throw new IllegalArgumentException(s"No cross-day batch cache demand profile for $hitPercent%")
      }
  }

  private final case class PlannedGet(request: GetRequest, expectedTimedValues: Int, expectedPayloadBytes: Long)

  private final case class RequestIdentity(
      dataset: String,
      keyBytes: Vector[Byte],
      startTsMillis: Option[Long],
      endTsMillis: Option[Long]
  )

  private val RangeStartMillis = Instant.parse("2026-02-01T00:00:00Z").toEpochMilli

  def build(config: Config = Config()): Workload = {
    val groupBys = buildGroupBys(config)
    val contextKey = "context_id=shared".getBytes(StandardCharsets.UTF_8)
    val candidateKeys = (1 to config.candidates).map { index =>
      f"candidate_id=$index%04d".getBytes(StandardCharsets.UTF_8)
    }.toVector
    val rangeEndMillis = RangeStartMillis + config.tiledPoints.toLong * config.tileSizeMillis - 1L

    val plannedLogicalReads = candidateKeys.flatMap { candidateKey =>
      groupBys.flatMap { groupBy =>
        val ownerKey = if (groupBy.ownership == Context) contextKey else candidateKey
        val batch = PlannedGet(
          GetRequest(ownerKey, groupBy.batchDataset),
          expectedTimedValues = 1,
          expectedPayloadBytes = groupBy.payloadBytes.toLong
        )
        groupBy.streamingDataset match {
          case None => Vector(batch)
          case Some(streamingDataset) =>
            val tileKey = TilingUtils.buildTileKey(streamingDataset, ownerKey, Some(config.tileSizeMillis), None)
            val points = groupBy.readShape.streamingPoints(config)
            Vector(
              batch,
              PlannedGet(
                GetRequest(
                  TilingUtils.serializeTileKey(tileKey),
                  streamingDataset,
                  Some(RangeStartMillis),
                  Some(rangeEndMillis)
                ),
                expectedTimedValues = points,
                expectedPayloadBytes = points.toLong * groupBy.payloadBytes
              )
            )
        }
      }
    }

    val uniqueReads = mutable.LinkedHashMap.empty[RequestIdentity, PlannedGet]
    plannedLogicalReads.foreach { planned =>
      uniqueReads.getOrElseUpdate(identity(planned.request), planned)
    }
    val plannedUniqueReads = uniqueReads.values.toVector
    val plannedCrossDayReads = plannedUniqueReads.map { planned =>
      if (planned.request.startTsMillis.isDefined) {
        planned.copy(request = planned.request.copy(
          startTsMillis = Some(RangeStartMillis - config.tileSizeMillis),
          endTsMillis = Some(rangeEndMillis - config.tileSizeMillis)
        ))
      } else {
        planned
      }
    }
    val plannedUniqueBatchReads = plannedUniqueReads.filter(_.request.startTsMillis.isEmpty)
    val batchCacheDemand = Vector(50, 80, 100).map { hitPercent =>
      val candidateMisses = math.ceil(config.candidates * (100 - hitPercent) / 100.0).toInt
      val missingCandidateKeys = candidateKeys.take(candidateMisses).iterator.map(_.toVector).toSet
      val reads = plannedUniqueReads.filter { planned =>
        planned.request.startTsMillis.isDefined || missingCandidateKeys.contains(planned.request.keyBytes.toVector)
      }
      profile(s"synthetic-context-hot-candidate-hit-$hitPercent", reads)
    }
    val batchCacheDemandCrossDay = Vector(50, 80, 100).map { hitPercent =>
      val candidateMisses = math.ceil(config.candidates * (100 - hitPercent) / 100.0).toInt
      val missingCandidateKeys = candidateKeys.take(candidateMisses).iterator.map(_.toVector).toSet
      val reads = plannedCrossDayReads.filter { planned =>
        planned.request.startTsMillis.isDefined || missingCandidateKeys.contains(planned.request.keyBytes.toVector)
      }
      profile(s"synthetic-context-hot-candidate-hit-$hitPercent-cross-day-24h", reads)
    }

    val puts = groupBys.flatMap { groupBy =>
      val ownerKeys = groupBy.ownership match {
        case Context   => Vector(contextKey)
        case Candidate => candidateKeys
      }
      ownerKeys.flatMap { ownerKey =>
        val batchPut = PutRequest(
          ownerKey,
          payload(groupBy, pointIndex = 0),
          groupBy.batchDataset,
          Some(RangeStartMillis)
        )
        groupBy.streamingDataset match {
          case None => Vector(batchPut)
          case Some(streamingDataset) =>
            val pointIndexes = groupBy.readShape match {
              case LastValue   => Seq(-1, config.tiledPoints - 1)
              case HourlyTiled => -1 until config.tiledPoints
              case Snapshot    => Seq.empty
            }
            val streamingPuts = pointIndexes.map { pointIndex =>
              val tileStartMillis = RangeStartMillis + pointIndex.toLong * config.tileSizeMillis
              val tileKey = TilingUtils.buildTileKey(
                streamingDataset,
                ownerKey,
                Some(config.tileSizeMillis),
                Some(tileStartMillis)
              )
              PutRequest(
                TilingUtils.serializeTileKey(tileKey),
                payload(groupBy, pointIndex),
                streamingDataset,
                Some(tileStartMillis)
              )
            }
            batchPut +: streamingPuts.toVector
        }
      }
    }

    val datasets = groupBys.flatMap(groupBy => groupBy.batchDataset +: groupBy.streamingDataset.toVector).distinct
    val uniqueBatchRequests = plannedUniqueReads.count(_.request.startTsMillis.isEmpty)
    val uniqueStreamingRequests = plannedUniqueReads.size - uniqueBatchRequests

    Workload(
      config,
      groupBys,
      datasets,
      puts,
      profile("logical", plannedLogicalReads),
      profile("deduplicated", plannedUniqueReads),
      profile("deduplicated-cross-day-24h", plannedCrossDayReads),
      profile("batch-only", plannedUniqueBatchReads),
      batchCacheDemand,
      batchCacheDemandCrossDay,
      uniqueBatchRequests,
      uniqueStreamingRequests
    )
  }

  private def buildGroupBys(config: Config): Vector[GroupBySpec] = {
    def specs(prefix: String,
              ownership: Ownership,
              shape: ReadShape,
              count: Int,
              payloadBytes: Int = config.scalarPayloadBytes): Vector[GroupBySpec] =
      (0 until count)
        .map(index => GroupBySpec(f"CHRONON_REDIS_BENCH_${prefix}_$index%02d", ownership, shape, payloadBytes))
        .toVector

    val candidateSnapshots =
      specs("CANDIDATE_EMBEDDING",
            Candidate,
            Snapshot,
            config.candidateEmbeddingGroupBys,
            config.embeddingPayloadBytes) ++
        specs(
          "CANDIDATE_SNAPSHOT",
          Candidate,
          Snapshot,
          config.candidateSnapshotGroupBys - config.candidateEmbeddingGroupBys
        )

    specs("CONTEXT_LAST", Context, LastValue, config.contextLastGroupBys) ++
      specs("CONTEXT_TILED", Context, HourlyTiled, config.contextTiledGroupBys) ++
      specs("CONTEXT_SNAPSHOT", Context, Snapshot, config.contextSnapshotGroupBys) ++
      specs("CANDIDATE_LAST", Candidate, LastValue, config.candidateLastGroupBys) ++
      specs("CANDIDATE_TILED", Candidate, HourlyTiled, config.candidateTiledGroupBys) ++
      candidateSnapshots
  }

  private def payload(groupBy: GroupBySpec, pointIndex: Int): Array[Byte] =
    Array.tabulate(groupBy.payloadBytes)(index => ((groupBy.name.hashCode + pointIndex + index) & 0xff).toByte)

  private def identity(request: GetRequest): RequestIdentity =
    RequestIdentity(request.dataset, request.keyBytes.toVector, request.startTsMillis, request.endTsMillis)

  private def profile(name: String, reads: Seq[PlannedGet]): ReadProfile =
    ReadProfile(
      name,
      reads.map(_.request).toVector,
      reads.map(_.expectedTimedValues.toLong).sum,
      reads.map(_.expectedPayloadBytes).sum
    )
}
