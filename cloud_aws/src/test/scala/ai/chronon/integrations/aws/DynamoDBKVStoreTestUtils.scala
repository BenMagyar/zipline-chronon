package ai.chronon.integrations.aws

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemRequest,
  BatchGetItemResponse,
  GetItemRequest,
  GetItemResponse,
  ReplicaStatus
}

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
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

/** Test helper that wraps PrefixedDynamoDbAsyncClient and counts BatchGetItem invocations,
  * so tests can verify chunking behavior (e.g. N=100 → 1 call, N=101 → 2 calls).
  */
class CountingPrefixedDynamoDbAsyncClient(delegate: DynamoDbAsyncClient, prefix: String)
    extends PrefixedDynamoDbAsyncClient(delegate, prefix) {
  val batchGetCallCount: AtomicInteger = new AtomicInteger(0)
  val batchGetChunkSizes: mutable.ListBuffer[Int] = mutable.ListBuffer.empty

  override def batchGetItem(request: BatchGetItemRequest): CompletableFuture[BatchGetItemResponse] = {
    batchGetCallCount.incrementAndGet()
    synchronized {
      val totalKeys = request
        .requestItems()
        .values()
        .stream()
        .mapToInt(_.keys().size())
        .sum()
      batchGetChunkSizes += totalKeys
    }
    super.batchGetItem(request)
  }
}

/** Subclass that installs a CountingPrefixedDynamoDbAsyncClient so tests can assert on chunking. */
class CountingBatchDynamoDBKVStore(client: DynamoDbAsyncClient, conf: Map[String, String] = Map.empty)
    extends DynamoDBKVStoreImpl(client, conf) {
  override protected def newPrefixedClient(delegate: DynamoDbAsyncClient,
                                           prefix: String): PrefixedDynamoDbAsyncClient =
    new CountingPrefixedDynamoDbAsyncClient(delegate, prefix)

  def batchGetCallCount: Int =
    prefixedDynamoDbClient.asInstanceOf[CountingPrefixedDynamoDbAsyncClient].batchGetCallCount.get()

  def batchGetChunkSizes: Seq[Int] =
    prefixedDynamoDbClient.asInstanceOf[CountingPrefixedDynamoDbAsyncClient].batchGetChunkSizes.toSeq
}

/** Wraps BatchGetItem so any request whose keys contain `poisonBytes` in the partition-key column
  * fails with the provided throwable. Used to exercise per-chunk failure isolation.
  */
class PoisonedBatchDynamoDbAsyncClient(delegate: DynamoDbAsyncClient,
                                       prefix: String,
                                       partitionKeyColumn: String,
                                       poisonBytes: Array[Byte],
                                       error: Throwable)
    extends PrefixedDynamoDbAsyncClient(delegate, prefix) {
  override def batchGetItem(request: BatchGetItemRequest): CompletableFuture[BatchGetItemResponse] = {
    val contains = request
      .requestItems()
      .values()
      .stream()
      .anyMatch { ka =>
        ka.keys().stream().anyMatch { key =>
          val av = key.get(partitionKeyColumn)
          av != null && java.util.Arrays.equals(av.b().asByteArray(), poisonBytes)
        }
      }
    if (contains) {
      val f = new CompletableFuture[BatchGetItemResponse]()
      f.completeExceptionally(error)
      f
    } else super.batchGetItem(request)
  }
}

class PoisonedChunkBatchDynamoDBKVStore(client: DynamoDbAsyncClient,
                                        conf: Map[String, String],
                                        poisonBytes: Array[Byte],
                                        error: Throwable)
    extends DynamoDBKVStoreImpl(client, conf) {
  override protected def newPrefixedClient(delegate: DynamoDbAsyncClient,
                                           prefix: String): PrefixedDynamoDbAsyncClient =
    new PoisonedBatchDynamoDbAsyncClient(delegate,
                                         prefix,
                                         DynamoDBKVStoreConstants.partitionKeyColumn,
                                         poisonBytes,
                                         error)
}

/** Wraps getItem to count calls whose table name matches the given target. Used to prove that a
  * request landing between scheduler ticks does NOT trigger an extra loader call.
  */
class CountingGetItemDynamoDbAsyncClient(delegate: DynamoDbAsyncClient, prefix: String, targetTable: String)
    extends PrefixedDynamoDbAsyncClient(delegate, prefix) {
  val targetGetItemCallCount: AtomicInteger = new AtomicInteger(0)

  override def getItem(request: GetItemRequest): CompletableFuture[GetItemResponse] = {
    if (request.tableName() == targetTable) targetGetItemCallCount.incrementAndGet()
    super.getItem(request)
  }
}

class CountingRegistryGetItemKVStore(client: DynamoDbAsyncClient, conf: Map[String, String] = Map.empty)
    extends DynamoDBKVStoreImpl(client, conf) {
  override protected def newPrefixedClient(delegate: DynamoDbAsyncClient,
                                           prefix: String): PrefixedDynamoDbAsyncClient =
    new CountingGetItemDynamoDbAsyncClient(delegate, prefix, DynamoDBKVStoreConstants.batchTableRegistry)

  def registryGetItemCount: Int =
    prefixedDynamoDbClient.asInstanceOf[CountingGetItemDynamoDbAsyncClient].targetGetItemCallCount.get()
}
