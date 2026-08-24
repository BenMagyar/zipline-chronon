package ai.chronon.integrations.aws

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ReplicaStatus

import scala.collection.mutable

/** Subclass of DynamoDBKVStoreImpl that records calls to addReplicaRegions instead of executing them.
  *
  * DynamoDB Local does not support Global Tables operations, so we can't verify replica update
  * behavior end-to-end. This spy captures (tableName, regions) pairs so tests can assert that
  * addReplicaRegions was called with the correct arguments — and that it was not called when config
  * is absent.
  */
class SpyingDynamoDBKVStore(client: DynamoDbAsyncClient, conf: Map[String, String] = Map.empty)
    extends DynamoDBKVStoreImpl(client, conf) {

  // Each element is (tableName, resolvedRegions, waitForActive) captured from an addReplicaRegions call
  val replicaCalls: mutable.ListBuffer[(String, List[String], Boolean)] = mutable.ListBuffer.empty

  override protected def addReplicaRegions(tableName: String, waitForActive: Boolean): Unit = {
    import ai.chronon.api.Constants.KvReplicaRegionsArg
    val regions = conf
      .get(KvReplicaRegionsArg)
      .filter(_.nonEmpty)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toList)
      .getOrElse(List.empty)
    // Mirror the real impl's early-exit: only record if there are actual regions to replicate to
    if (regions.nonEmpty) replicaCalls += ((tableName, regions, waitForActive))
  }
}

/** Subclass that stubs the replica-status polling seam so tests can drive waitForReplicasActive
  * through a scripted sequence of DescribeTable responses without a real Global Table.
  *
  * `statusSequence` is consumed one entry per poll; the last entry sticks if the wait outlasts
  * the script. Poll interval is set to 0 so tests don't sleep between iterations.
  */
class ScriptedReplicaStatusKVStore(client: DynamoDbAsyncClient,
                                   conf: Map[String, String],
                                   statusSequence: Seq[Map[String, ReplicaStatus]])
    extends DynamoDBKVStoreImpl(client, conf) {
  private val remaining: mutable.Queue[Map[String, ReplicaStatus]] = mutable.Queue(statusSequence: _*)
  var pollCount: Int = 0

  override private[aws] def fetchReplicaStatuses(tableName: String): Map[String, ReplicaStatus] = {
    pollCount += 1
    if (remaining.size > 1) remaining.dequeue() else remaining.headOption.getOrElse(Map.empty)
  }

  override protected def replicaWaitPollIntervalMs: Long = 0L
}

/** Subclass that forces addReplicaRegions to throw. Used to exercise the bulkPut boundary case
  * where replica setup fails and the batch-table registry pointer must NOT be flipped.
  */
class ReplicaFailingKVStore(client: DynamoDbAsyncClient, conf: Map[String, String] = Map.empty)
    extends DynamoDBKVStoreImpl(client, conf) {
  override protected def addReplicaRegions(tableName: String, waitForActive: Boolean): Unit =
    throw new RuntimeException(s"Simulated replica failure for '$tableName'")
}
