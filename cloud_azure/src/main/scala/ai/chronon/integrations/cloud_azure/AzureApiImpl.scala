package ai.chronon.integrations.cloud_azure

import ai.chronon.integrations.redis.RedisKVStoreFactory
import ai.chronon.online._
import ai.chronon.online.serde.{AvroConversions, AvroSerDe, SerDe}

import java.util.concurrent.atomic.AtomicReference

/** Implementation of Chronon's API interface for Azure.
  *
  * Supports multiple KV store backends based on configuration:
  *   - Redis (default): Set via REDIS_CLUSTER_NODES or redis.cluster.nodes
  *   - Cosmos DB: Set kv.store.type=cosmos or KV_STORE_TYPE=cosmos
  *
  * Redis Configuration:
  *   - REDIS_CLUSTER_NODES: Comma-separated cluster nodes (e.g., "node1:6379,node2:6379") [required]
  *   - REDIS_PASSWORD: Redis password (optional)
  *   - See RedisKVStoreFactory for additional configuration options.
  *
  * Cosmos DB Configuration:
  *   - COSMOS_ENDPOINT: Cosmos DB account endpoint [required]
  *   - COSMOS_KEY: Cosmos DB master key [required]
  *   - COSMOS_DATABASE: Cosmos DB database name (default: "chronon")
  *   - See CosmosKVStoreFactory for additional configuration options.
  */
class AzureApiImpl(conf: Map[String, String]) extends Api(conf) {

  import AzureApiImpl._

  override def genKvStore: KVStore = {
    try {
      Option(sharedKvStore.get()) match {
        case Some(existingStore) =>
          existingStore
        case None =>
          kvStoreLock.synchronized {
            Option(sharedKvStore.get()) match {
              case Some(existingStore) => existingStore
              case None =>
                val kvStoreType = conf.getOrElse("kv.store.type", sys.env.getOrElse("KV_STORE_TYPE", "cosmos"))

                val newStore = kvStoreType.toLowerCase match {
                  case "cosmos" =>
                    logger.info("Initializing Cosmos DB KV store")
                    val store = CosmosKVStoreFactory.create(conf)
                    logger.info("Successfully created Cosmos DB KV store")
                    store
                  case "redis" =>
                    logger.info("Initializing Redis KV store")
                    RedisKVStoreFactory.createFullSnapshot(conf)
                  case other =>
                    throw new IllegalArgumentException(
                      s"Unsupported KV store type: $other. Supported types: cosmos, redis")
                }

                sharedKvStore.set(newStore)
                newStore
            }
          }
      }
    } catch {
      case e: IllegalArgumentException =>
        logger.error(s"Failed to initialize KV store due to configuration error: ${e.getMessage}", e)
        throw e
      case e: Exception =>
        logger.error(s"Failed to initialize KV store: ${e.getMessage}", e)
        throw e
    }
  }

  override def genMetricsKvStore(tableBaseName: String): KVStore = null

  override def genEnhancedStatsKvStore(tableBaseName: String): KVStore = null

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): SerDe =
    new AvroSerDe(AvroConversions.fromChrononSchema(groupByServingInfoParsed.streamChrononSchema))

  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  override def externalRegistry: ExternalSourceRegistry = registry

  override def logResponse(resp: LoggableResponse): Unit = {
    logger.debug(s"LogResponse called for join ${resp.joinName} at ts ${resp.tsMillis}")
  }
}

object AzureApiImpl {
  private val sharedKvStore = new AtomicReference[KVStore]()
  private val kvStoreLock = new Object()

  private val sharedMetricsKvStore = new AtomicReference[KVStore]()
  private val metricsKvStoreLock = new Object()

  private val sharedEnhancedStatsKvStore = new AtomicReference[KVStore]()
  private val enhancedStatsKvStoreLock = new Object()
}
