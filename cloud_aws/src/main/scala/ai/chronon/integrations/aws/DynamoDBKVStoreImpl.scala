package ai.chronon.integrations.aws

import ai.chronon.api.Constants.{
  ContinuationKey,
  DynamoDbReplicaWaitTimeoutMsKey,
  KvEnableTtlArg,
  KvReplicaRegionsArg,
  KvTablePrefixArg,
  KvUploadBatchTableGCAgeDaysKey,
  KvUploadTimeoutMsKey,
  ListLimit
}
import ai.chronon.integrations.aws.AwsApiImpl.DynamoBatchRegistryRefreshIntervalMs
import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Constants, PartitionSpec, TilingUtils}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore._
import ai.chronon.online.metrics.Metrics.Context
import ai.chronon.online.metrics.{Metrics, TTLCache}
import ai.chronon.spark.{IonPathConfig, IonWriter}
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import java.nio.charset.Charset
import java.time.{Duration, Instant, LocalDate}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{
  CompletableFuture,
  CompletionException,
  ScheduledFuture,
  ScheduledThreadPoolExecutor,
  TimeUnit
}
import scala.collection.mutable
import scala.compat.java8.FutureConverters
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class DynamoDBKVStoreImpl(rawDynamoDbClient: DynamoDbAsyncClient, conf: Map[String, String] = Map.empty)
    extends KVStore {
  import DynamoDBKVStoreConstants._

  protected val enableTtl: Boolean = conf.getOrElse(KvEnableTtlArg, "true").toBoolean

  private val tablePrefix = conf.getOrElse(KvTablePrefixArg, "")

  private val replicaRegions: List[String] =
    conf
      .get(KvReplicaRegionsArg)
      .filter(_.nonEmpty)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toList)
      .getOrElse(List.empty)

  protected val batchGetEnabled: Boolean =
    getOptional(DynamoEnableBatchGetKey, conf).exists(_.toBoolean)

  protected def newPrefixedClient(delegate: DynamoDbAsyncClient, prefix: String): PrefixedDynamoDbAsyncClient =
    new PrefixedDynamoDbAsyncClient(delegate, prefix)

  // Wrap the client to automatically prefix all table names
  protected val prefixedDynamoDbClient: PrefixedDynamoDbAsyncClient = {
    logger.info(
      s"Using: table prefix: '$tablePrefix' (prefix will be added to all table names used by this KVStore); enableTtl: $enableTtl; batchGetEnabled: $batchGetEnabled")
    newPrefixedClient(rawDynamoDbClient, tablePrefix)
  }

  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("dynamodb")

  // Scheduled refresher drives cache freshness; request-path refresh() is a fallback for scheduler
  // gaps and cold-start reads. Cache's refresh interval is tuned to 2x scheduler interval so a request
  // landing between ticks doesn't enqueue a redundant refresh in the steady state. When the scheduler
  // is disabled, fall back to the pre-scheduler 8s cadence so request-driven refreshes remain the sole
  // freshness source.
  protected def batchRegistryRefreshIntervalMs: Long =
    getOptional(DynamoBatchRegistryRefreshIntervalMs, conf)
      .map(_.toLong)
      .getOrElse(DefaultBatchRegistryRefreshIntervalMs)

  private val batchTableCacheRefreshMs: Long =
    if (batchRegistryRefreshIntervalMs > 0) batchRegistryRefreshIntervalMs * 2
    else BatchTableCacheRefreshIntervalWhenSchedulerDisabledMs

  // TTLCache: resolves logical batch dataset names to physical date-suffixed table names
  private[aws] val batchTableCache: TTLCache[String, String] = new TTLCache[String, String](
    f = { dataset =>
      val keyMap = Map(partitionKeyColumn -> AttributeValue.builder.b(SdkBytes.fromByteArray(dataset.getBytes)).build)
      val request = GetItemRequest.builder
        .tableName(batchTableRegistry)
        .key(keyMap.toJava)
        .build

      val item = prefixedDynamoDbClient.getItem(request).join().item().toScala
      item.get("valueBytes").map(v => new String(v.b().asByteArray())).getOrElse(dataset)
    },
    contextBuilder = { _ => metricsContext.withSuffix("batch_table_cache") },
    refreshIntervalMillis = batchTableCacheRefreshMs
  )

  private[aws] def resolveTableName(dataset: String): String = {
    // refresh() is fallback: steady-state refresh is driven by the scheduled refresher below, but this
    // self-heals if the scheduler misses a tick (GC pause, one bad key stalling the runnable, etc.).
    if (dataset.endsWith(batchSuffix)) batchTableCache.refresh(dataset)
    else dataset
  }

  // Iterates every batch dataset the cache has seen and forces a refresh. Runs on the isolated TTLCache
  // refresh pool via force() → asyncUpdateOnExpiry(_, 0). Per-key try/catch so one bad key can't kill
  // the schedule (loader exceptions are also swallowed inside TTLCache's runnable).
  private[aws] def refreshBatchRegistryOnce(): Unit = {
    val it = batchTableCache.cMap.keySet().iterator()
    while (it.hasNext) {
      val key = it.next()
      try {
        batchTableCache.force(key)
      } catch {
        case e: Exception =>
          logger.warn(s"Scheduled batch registry refresh failed for key '$key'", e)
      }
    }
  }

  private val batchRegistryHandle: Option[BatchRegistryScheduler.Handle] = {
    val intervalMs = batchRegistryRefreshIntervalMs
    if (intervalMs <= 0) {
      logger.info(s"Scheduled batch registry refresh disabled ($DynamoBatchRegistryRefreshIntervalMs=$intervalMs)")
      None
    } else {
      val handle = BatchRegistryScheduler.register(
        intervalMs,
        () => refreshBatchRegistryOnce(),
        onError = e => logger.warn("Scheduled batch registry refresh tick failed", e)
      )
      logger.info(s"Scheduled batch registry refresh every ${intervalMs}ms")
      Some(handle)
    }
  }

  def close(): Unit = batchRegistryHandle.foreach(BatchRegistryScheduler.unregister)

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  private def tableExists(dataset: String): Boolean = {
    val request = DescribeTableRequest.builder.tableName(dataset).build
    try {
      prefixedDynamoDbClient.describeTable(request).join()
      true
    } catch {
      case _: ResourceNotFoundException                                                 => false
      case e: CompletionException if e.getCause.isInstanceOf[ResourceNotFoundException] => false
    }
  }

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    if (tableExists(dataset)) {
      logger.info(s"DynamoDB table $dataset already exists, skipping creation")
      // Retry safety: a prior create() may have created the table but failed before replicas
      // were requested. Reconcile now — but don't block on ACTIVE; create() has no atomic pointer
      // swap to protect, and blocking multi-minutes here can trip Vert.x worker-thread checkers.
      addReplicaRegions(dataset, waitForActive = false)
      return
    }

    val maybeSortKeys = props.get(isTimedSorted) match {
      case Some(value: String) if value.toLowerCase == "true" => Some(sortKeyColumn)
      case Some(value: Boolean) if value                      => Some(sortKeyColumn)
      case _ if isStreamingTable(dataset)                     => Some(sortKeyColumn)
      case _                                                  => None
    }

    val keyAttributes =
      Seq(AttributeDefinition.builder.attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build) ++
        maybeSortKeys.map(k => AttributeDefinition.builder.attributeName(k).attributeType(ScalarAttributeType.N).build)

    val keySchema =
      Seq(KeySchemaElement.builder.attributeName(partitionKeyColumn).keyType(KeyType.HASH).build) ++
        maybeSortKeys.map(p => KeySchemaElement.builder.attributeName(p).keyType(KeyType.RANGE).build)

    val request =
      CreateTableRequest.builder
        .attributeDefinitions(keyAttributes.toList.toJava)
        .keySchema(keySchema.toList.toJava)
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .tableName(dataset)
        .build

    logger.info(s"Triggering creation of DynamoDb table: $dataset with prefix '$tablePrefix' added later")
    try {
      prefixedDynamoDbClient.createTable(request).join()
      val waiterResponse = prefixedDynamoDbClient.waitUntilTableExists(dataset).join()
      if (waiterResponse.matched.exception().isPresent)
        throw waiterResponse.matched.exception().get()

      val tableDescription = waiterResponse.matched().response().get().table()
      logger.info(s"Table created successfully! Details: \n${tableDescription.toString}")

      if (enableTtl) {
        val ttlSpec = TimeToLiveSpecification.builder
          .enabled(true)
          .attributeName("ttl")
          .build
        val ttlRequest = UpdateTimeToLiveRequest.builder
          .tableName(dataset)
          .timeToLiveSpecification(ttlSpec)
          .build
        prefixedDynamoDbClient.updateTimeToLive(ttlRequest).join()
        logger.info(s"TTL enabled on table: $dataset with attribute 'ttl'")
      }

      // Non-atomic caller: fire-and-forget replica creation. See addReplicaRegions comment.
      addReplicaRegions(dataset, waitForActive = false)

      metricsContext.increment("create.successes")
    } catch {
      case _: ResourceInUseException =>
        logger.info(s"Table: $dataset already exists")
      case e: CompletionException if e.getCause.isInstanceOf[ResourceInUseException] =>
        logger.info(s"Table: $dataset already exists")
      case e: Exception =>
        logger.error(s"Error creating Dynamodb table: $dataset", e)
        metricsContext.increment("create.failures")
        throw e
    }
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    // partition our requests into pure get style requests (where we only have key lookup)
    // and query requests (we want to query a range based on afterTsMillis -> endTsMillis or now() )
    val (getLookups, queryLookups) = requests.partition(r => r.startTsMillis.isEmpty)
    val getItemResults = doGetLookups(getLookups)
    val aggregatedQueryResults = doQueryLookups(queryLookups)

    Future.sequence(getItemResults ++ aggregatedQueryResults)
  }

  protected def doGetLookups(getLookups: Seq[KVStore.GetRequest]): Seq[Future[GetResponse]] =
    if (batchGetEnabled) doBatchGetLookups(getLookups)
    else doSingleGetLookups(getLookups)

  protected def doSingleGetLookups(getLookups: Seq[KVStore.GetRequest]): Seq[Future[GetResponse]] = {
    val getItemCompletables = getLookups.map { req =>
      val keyAttributeMap = primaryKeyMap(req.keyBytes)
      val tableName = resolveTableName(req.dataset)
      val getItemReq = GetItemRequest.builder.key(keyAttributeMap.toJava).tableName(tableName).build
      val startTs = System.currentTimeMillis()
      (req, prefixedDynamoDbClient.getItem(getItemReq), startTs)
    }

    // timestamp to use for all get responses when the underlying tables don't have a ts field
    val defaultTimestamp = Instant.now().toEpochMilli

    val getItemResults = getItemCompletables.map { case (req, completableFuture, startTs) =>
      handleDynamoDbOperation(metricsContext.withSuffix("multiget"), req.dataset, startTs)(completableFuture)
        .transform {
          case Success(response) =>
            val resultValue = extractTimedValues(List(response.item()).toJava, defaultTimestamp)
            Success(GetResponse(req, resultValue))
          case Failure(e) =>
            Success(GetResponse(req, Failure(e)))
        }
    }
    getItemResults
  }

  // UnprocessedKeys returned by DynamoDB (partial throttling, 16MB payload cap, >1MB per partition)
  // become per-key Failure rather than triggering a retry — retry-with-backoff is deliberately
  // deferred for simplicity at the moment.
  protected def doBatchGetLookups(getLookups: Seq[KVStore.GetRequest]): Seq[Future[GetResponse]] = {
    if (getLookups.isEmpty) return Seq.empty

    val defaultTimestamp = Instant.now().toEpochMilli
    val resolved = getLookups.map(ResolvedLookup(_, resolveTableName))

    // One Future per chunk; each Future carries a Try so a failed chunk only fails the
    // requests whose keys were in that chunk. At 6k keys / 100 per chunk this is O(60)
    // callback allocations instead of O(6000) for a per-key future graph.
    val chunks: Seq[Seq[(String, KeyWrapper)]] =
      dedupedTableKeyPairs(resolved).grouped(BatchGetItemMaxKeys).toSeq
    val chunkResults: Seq[Future[Try[Map[(String, KeyWrapper), ChunkKeyOutcome]]]] =
      chunks.map { chunk =>
        fetchChunk(chunk)
          .map(Success(_): Try[Map[(String, KeyWrapper), ChunkKeyOutcome]])
          .recover { case e => Failure(e) }
      }

    val keyToChunkResult: Map[(String, KeyWrapper), Future[Try[Map[(String, KeyWrapper), ChunkKeyOutcome]]]] =
      chunks.iterator
        .zip(chunkResults.iterator)
        .flatMap { case (chunk, fut) =>
          chunk.iterator.map(pair => pair -> fut)
        }
        .toMap

    resolved.map(r => assembleResponse(r, keyToChunkResult((r.table, r.key)), defaultTimestamp))
  }

  // DynamoDB rejects duplicate keys within a single BatchGetItem with ValidationException, so
  // per-table dedup is required.
  private def dedupedTableKeyPairs(resolved: Seq[ResolvedLookup]): Seq[(String, KeyWrapper)] =
    resolved.map(r => (r.table, r.key)).distinct

  private def fetchChunk(chunk: Seq[(String, KeyWrapper)]): Future[Map[(String, KeyWrapper), ChunkKeyOutcome]] = {
    val request = buildBatchGetRequest(chunk)
    val startTs = System.currentTimeMillis()
    // Distinct suffix so per-batch latency doesn't distort the existing per-key `multiget` histogram.
    val tableSummary = chunk.map(_._1).distinct.mkString(",")
    handleDynamoDbOperation(metricsContext.withSuffix("multiget_batch"), tableSummary, startTs)(
      prefixedDynamoDbClient.batchGetItem(request)
    ).map(response => chunkLookupFromResponse(chunk, response))
  }

  private def assembleResponse(resolved: ResolvedLookup,
                               chunkResult: Future[Try[Map[(String, KeyWrapper), ChunkKeyOutcome]]],
                               defaultTimestamp: Long): Future[GetResponse] =
    chunkResult.map {
      case Success(lookup) =>
        lookup.get((resolved.table, resolved.key)) match {
          case Some(ChunkKeyOutcome.Item(item)) =>
            GetResponse(resolved.req, extractTimedValues(List(item).toJava, defaultTimestamp))
          case Some(ChunkKeyOutcome.Unprocessed) =>
            GetResponse(
              resolved.req,
              Failure(
                new RuntimeException(s"BatchGetItem returned UnprocessedKey for dataset '${resolved.req.dataset}'")))
          case None =>
            // Shouldn't happen — chunkLookupFromResponse populates every requested pair.
            GetResponse(
              resolved.req,
              Failure(new RuntimeException(s"BatchGetItem chunk missing entry for dataset '${resolved.req.dataset}'")))
        }
      case Failure(e) =>
        GetResponse(resolved.req, Failure(e))
    }

  private def buildBatchGetRequest(chunk: Seq[(String, KeyWrapper)]): BatchGetItemRequest = {
    val requestItems: util.Map[String, KeysAndAttributes] = chunk
      .groupBy(_._1)
      .map { case (table, pairs) =>
        val keys = pairs.map(p => primaryKeyMap(p._2.bytes).toJava).toList.toJava
        table -> KeysAndAttributes.builder.keys(keys).build
      }
      .toJava
    BatchGetItemRequest.builder.requestItems(requestItems).build
  }

  // Turns a BatchGetItemResponse into a (table, key) -> ChunkKeyOutcome lookup for this chunk.
  //   - Missing key (requested, no match) -> Item(empty map) so extractTimedValues yields Success(Seq.empty).
  //     Reuses a shared Collections.emptyMap since extractTimedValues doesn't mutate.
  //   - UnprocessedKey -> Unprocessed, so assembleResponse can surface a per-request Failure that
  //     names the caller's logical dataset (not the resolved physical table).
  private def chunkLookupFromResponse(chunk: Seq[(String, KeyWrapper)],
                                      response: BatchGetItemResponse): Map[(String, KeyWrapper), ChunkKeyOutcome] = {
    val items = mutable.Map.empty[(String, KeyWrapper), util.Map[String, AttributeValue]]
    if (response.hasResponses) {
      response.responses().forEach { (tableName, tableItems) =>
        tableItems.forEach { item =>
          Option(item.get(partitionKeyColumn))
            .map(av => KeyWrapper(av.b().asByteArray()))
            .foreach(k => items.update((tableName, k), item))
        }
      }
    }
    val unprocessed = mutable.Set.empty[(String, KeyWrapper)]
    if (response.hasUnprocessedKeys) {
      response.unprocessedKeys().forEach { (tableName, ka) =>
        ka.keys().forEach { key =>
          Option(key.get(partitionKeyColumn))
            .map(av => KeyWrapper(av.b().asByteArray()))
            .foreach(k => unprocessed.add((tableName, k)))
        }
      }
    }
    val emptyItem = java.util.Collections.emptyMap[String, AttributeValue]()
    chunk.iterator.map { pair =>
      val outcome: ChunkKeyOutcome =
        if (unprocessed.contains(pair)) ChunkKeyOutcome.Unprocessed
        else ChunkKeyOutcome.Item(items.getOrElse(pair, emptyItem))
      pair -> outcome
    }.toMap
  }

  protected def queryPartition(dataset: String,
                               partitionKeyBytes: Array[Byte],
                               startTs: Long,
                               endTs: Option[Long]): Future[QueryResponse] = {
    val queryRequest = buildTimeRangeQuery(dataset, partitionKeyBytes, startTs, endTs)
    val callStartTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("query"), dataset, callStartTs)(
      prefixedDynamoDbClient.query(queryRequest)
    )
  }

  protected def queryPartitionOnly(dataset: String, partitionKeyBytes: Array[Byte]): Future[QueryResponse] = {
    val queryRequest = buildPartitionOnlyQuery(dataset, partitionKeyBytes)
    val callStartTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("query"), dataset, callStartTs)(
      prefixedDynamoDbClient.query(queryRequest)
    )
  }

  protected def doQueryLookups(queryLookups: Seq[KVStore.GetRequest]): Seq[Future[GetResponse]] = {
    val defaultTimestamp = Instant.now().toEpochMilli

    queryLookups.map { req =>
      val resolvedDataset = resolveTableName(req.dataset)
      val tileComponents = extractTileKeyComponents(req.keyBytes)
      val endTs = req.endTsMillis.getOrElse(System.currentTimeMillis())
      val partitionKeys = generateTimeSeriesKeys(
        tileComponents.baseKeyBytes,
        req.startTsMillis.get,
        endTs,
        tileComponents.tileSizeMillis
      )

      // Optimize for the common case of a single partition key (queries within one day)
      if (partitionKeys.length == 1) {
        queryPartition(resolvedDataset, partitionKeys.head, req.startTsMillis.get, req.endTsMillis)
          .transform {
            case Success(response) =>
              val timedValues = extractTimedValues(response.items(), defaultTimestamp).getOrElse(Seq.empty)
              Success(GetResponse(req, Success(timedValues)))
            case Failure(e) =>
              Success(GetResponse(req, Failure(e)))
          }
      } else {
        // Multi-day query: fan out to multiple partition keys
        val queryFutures = partitionKeys.map { partitionKeyBytes =>
          queryPartition(resolvedDataset, partitionKeyBytes, req.startTsMillis.get, req.endTsMillis)
        }

        Future.sequence(queryFutures).transform {
          case Success(responses) =>
            val allTimedValues = responses.flatMap { response =>
              extractTimedValues(response.items(), defaultTimestamp).getOrElse(Seq.empty)
            }
            Success(GetResponse(req, Success(allTimedValues)))
          case Failure(e) =>
            Success(GetResponse(req, Failure(e)))
        }
      }
    }
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    val listLimit = request.props.get(ListLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => 100
    }

    val maybeExclusiveStartKey = request.props.get(ContinuationKey)
    val maybeExclusiveStartKeyAttribute = maybeExclusiveStartKey.map { k =>
      AttributeValue.builder.b(SdkBytes.fromByteArray(k.asInstanceOf[Array[Byte]])).build
    }

    val scanBuilder = ScanRequest.builder.tableName(request.dataset).limit(listLimit)
    val scanRequest = maybeExclusiveStartKeyAttribute match {
      case Some(value) => scanBuilder.exclusiveStartKey(Map(partitionKeyColumn -> value).toJava).build
      case _           => scanBuilder.build
    }

    val startTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("list"), request.dataset, startTs)(
      prefixedDynamoDbClient.scan(scanRequest)
    ).map { scanResponse =>
      val resultElements = extractListValues(scanResponse)
      val noPagesLeftResponse = ListResponse(request, resultElements, Map.empty)
      if (scanResponse.hasLastEvaluatedKey) {

        val lastEvalKey = scanResponse.lastEvaluatedKey().toScala.get(partitionKeyColumn)
        lastEvalKey match {
          case Some(av) => ListResponse(request, resultElements, Map(ContinuationKey -> av.b().asByteArray()))
          case _        => noPagesLeftResponse
        }
      } else {
        noPagesLeftResponse
      }
    }.recover { case e: Exception =>
      ListResponse(request, Failure(e), Map.empty)
    }
  }

  // Dynamo has restrictions on the number of requests per batch (and the payload size) as well as some partial
  // success behavior on batch writes which necessitates a bit more logic on our end to tie things together.
  // To keep things simple for now, we implement the multiput as a sequence of put calls.
  override def multiPut(keyValueDatasets: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.debug(s"Triggering multiput for ${keyValueDatasets.size}: rows")
    val futureResponses = keyValueDatasets.map { req =>
      val (actualKeyBytes, actualTimestamp) = if (isStreamingTable(req.dataset)) {
        // For streaming tables, unwrap TileKey to use entity key + tileSizeMs as partition key
        // and tileStartTs as sort key. Including tileSizeMs in the key supports tile layering.
        val tileComponents = extractTileKeyComponents(req.keyBytes)
        val timestamp = tileComponents.tileStartTimestampMillis
        val tiledKey = buildKeyWithTileSize(tileComponents.baseKeyBytes, timestamp, tileComponents.tileSizeMillis)
        (tiledKey, timestamp)
      } else {
        val timestampInPutRequest = req.tsMillis.getOrElse(System.currentTimeMillis())
        (req.keyBytes, timestampInPutRequest)
      }

      val attributeMap: Map[String, AttributeValue] = buildAttributeMap(actualKeyBytes, req.valueBytes)
      val tsMap = Map(sortKeyColumn -> AttributeValue.builder.n(actualTimestamp.toString).build)
      val ttlMap = if (enableTtl) {
        val ttlSeconds = (System.currentTimeMillis() / 1000).toInt + DataTTLSeconds
        Map("ttl" -> AttributeValue.builder.n(ttlSeconds.toString).build)
      } else Map.empty[String, AttributeValue]

      val putItemReq =
        PutItemRequest.builder.tableName(req.dataset).item((attributeMap ++ tsMap ++ ttlMap).toJava).build()
      val startTs = System.currentTimeMillis()
      handleDynamoDbOperation(metricsContext.withSuffix("multiput"), req.dataset, startTs)(
        prefixedDynamoDbClient.putItem(putItemReq)
      ).transform {
        case Success(_) => Success(true)
        case Failure(_) => Success(false)
      }
    }
    Future.sequence(futureResponses)
  }

  /** Bulk loads data from S3 Ion files into DynamoDB using the ImportTable API.
    *
    * The Ion files are expected to have been written by IonWriter during GroupByUpload.
    * The S3 location is determined by IonWriter.resolveS3Location using:
    *   - Root path from config: spark.chronon.table_write.upload.root_path
    *   - Dataset name: sourceOfflineTable (e.g., namespace.groupby_v1__upload)
    *   - Partition column and value: ds={partition}
    *
    * Full path: s3://{spark.chronon.table_write.upload.root_path}/{sourceOfflineTable}/ds={partition}/
    *
    * Creates a date-suffixed physical table (e.g. MY_GROUPBY_BATCH_2026_02_17) and registers the
    * mapping from logical dataset name to physical table in CHRONON_BATCH_TABLE_REGISTRY.
    */
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val rootPath = conf.get(IonPathConfig.UploadLocationKey)
    val partitionColumn = conf.getOrElse(IonPathConfig.PartitionColumnKey, IonPathConfig.DefaultPartitionColumn)

    // Use shared IonWriter path resolution to ensure consistency between producer and consumer
    val path = IonWriter.resolvePartitionPath(sourceOfflineTable, partitionColumn, partition, rootPath)
    val s3Source = toS3BucketSource(path)
    val logicalTableName = destinationOnlineDataSet
    val timestamp = Instant.now().toEpochMilli
    val physicalTableName =
      logicalTableName.sanitize.toUpperCase + "_" + partition.replace("-", "_") + "_" + timestamp
    logger.info(
      s"Starting DynamoDB import for table: $physicalTableName (logical: $logicalTableName) from S3: $s3Source with prefix '$tablePrefix' added later")

    val tableParams = TableCreationParameters
      .builder()
      .tableName(physicalTableName)
      .keySchema(
        KeySchemaElement.builder().attributeName(partitionKeyColumn).keyType(KeyType.HASH).build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .build()

    val importRequest = ImportTableRequest
      .builder()
      .s3BucketSource(s3Source)
      .inputFormat(InputFormat.ION)
      .inputCompressionType(InputCompressionType.NONE)
      .tableCreationParameters(tableParams)
      .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
      .build()

    try {
      val startTs = System.currentTimeMillis()
      val importResponse = prefixedDynamoDbClient.importTable(importRequest).join()
      val importArn = importResponse.importTableDescription().importArn()

      logger.info(s"DynamoDB import initiated with ARN: $importArn for table: $physicalTableName")

      waitForImportCompletion(importArn, physicalTableName, configuredImportTimeout)

      finalizeBulkPut(logicalTableName, physicalTableName)

      val duration = System.currentTimeMillis() - startTs
      logger.info(s"DynamoDB import completed for table: $physicalTableName in ${duration}ms")
      metricsContext.increment("bulkPut.successes")
      metricsContext.distribution("bulkPut.latency", duration)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to import data to DynamoDB table: $physicalTableName", e)
        metricsContext.increment("bulkPut.failures")
        throw e
    }
  }

  // Post-import steps for bulkPut. Extracted so tests can drive the replica → registry ordering
  // without also needing to stub out ImportTable (which requires S3). The registry pointer flip
  // MUST come after addReplicaRegions returns successfully — otherwise readers in replica regions
  // can see the new physical table name before the replica exists.
  private[aws] def finalizeBulkPut(logicalTableName: String, physicalTableName: String): Unit = {
    // ImportTable API does not support TTL configuration; must be applied after import completes
    if (enableTtl) {
      val ttlSpec = TimeToLiveSpecification.builder.enabled(true).attributeName("ttl").build
      val ttlRequest = UpdateTimeToLiveRequest.builder
        .tableName(physicalTableName)
        .timeToLiveSpecification(ttlSpec)
        .build
      prefixedDynamoDbClient.updateTimeToLive(ttlRequest).join()
      logger.info(s"TTL enabled on imported table: $physicalTableName")
    }

    // Registry pointer flip follows — must block until replicas are ACTIVE, or readers routed to
    // a replica region would hit ResourceNotFoundException on the freshly swapped physical table.
    addReplicaRegions(physicalTableName, waitForActive = true)

    // Register the physical table name in the batch table registry
    create(batchTableRegistry)
    val registryKey = logicalTableName.sanitize.toUpperCase + batchSuffix
    Await.result(
      multiPut(Seq(KVStore.PutRequest(registryKey.getBytes, physicalTableName.getBytes, batchTableRegistry))),
      30.seconds
    )
    logger.info(s"Registry updated: $registryKey -> $physicalTableName")

    cleanupTables(KVStore.CleanupRequest(datasetName = Some(logicalTableName)))
  }

  /** Scoped mode ([[KVStore.CleanupRequest.datasetName]] set): ASCII-ordered ListTables pagination
    * starts at the prefix and bails as soon as names diverge. Within a scoped prefix the
    * YYYY_MM_DD_epoch tail means names sort oldest-first — the earliest `maxDelete` matches ARE
    * the oldest ones.
    *
    * Unscoped mode: pages the whole account, filtering by the [[BatchTableNameRegex]] and age
    * predicate on the fly. Stops as soon as `maxDelete` deletion-eligible tables are collected.
    * Deletes the first-alphabetical `maxDelete` stale tables per run
    *
    * Returns the number of tables actually deleted. Never throws — per-table and top-level errors
    * are logged and swallowed so cleanup never blocks the caller.
    */
  override def cleanupTables(request: KVStore.CleanupRequest): Int = {
    def gcCutoffDate: LocalDate = {
      val cleanupDays = conf.get(KvUploadBatchTableGCAgeDaysKey).map(_.toLong).getOrElse(BatchTableGCAgeDays.toLong)
      LocalDate.now().minusDays(cleanupDays)
    }

    if (!enableTtl) return 0
    val defaultMax = request.datasetName.fold(BatchTableSweepMaxDelete)(_ => BatchTableGCMaxDelete)
    val maxDelete = request.maxDelete.getOrElse(defaultMax)
    val prefix = request.datasetName.map(_.sanitize.toUpperCase + "_")
    val mode = if (prefix.isDefined) "scoped" else "sweep"
    val modeTag = Map("mode" -> mode)
    val cleanupMetrics = metricsContext.withSuffix("cleanup")
    val logContext = prefix.map(p => s"Batch table cleanup for prefix $p").getOrElse("Batch table sweep")
    val cycleStartTs = System.currentTimeMillis()
    cleanupMetrics.increment("cycles", modeTag)
    try {
      val cutoff = gcCutoffDate
      val toDelete = collectDeletionCandidates(prefix, cutoff, maxDelete)
      cleanupMetrics.distribution("candidates_found", toDelete.size.toLong, modeTag)
      logger.info(s"$logContext: deleting ${toDelete.size} table(s) (cap=$maxDelete)")
      var deleted = 0
      toDelete.foreach { tableName =>
        val physicalName = prefixedDynamoDbClient.prefixTableName(tableName)
        try {
          if (removeReplicasThenDelete(tableName, modeTag)) {
            logger.info(s"Deleted old batch table: $physicalName")
            cleanupMetrics.increment("deleted", modeTag)
            deleted += 1
          }
          // False = replica removal was kicked off but the table itself isn't gone yet; a
          // subsequent cleanup cycle picks it up once replicas are removed.
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to delete old batch table: $physicalName", e)
            cleanupMetrics.increment("delete_failures", modeTag)
        }
      }
      deleted
    } catch {
      case e: Exception =>
        logger.warn(s"$logContext failed, skipping", e)
        cleanupMetrics.increment("cycle_failures", modeTag)
        0
    } finally {
      cleanupMetrics.distribution("cycle_latency_ms", System.currentTimeMillis() - cycleStartTs, modeTag)
    }
  }

  // DeleteTable on a global-table primary while replicas exist returns ResourceInUseException.
  // Fire-and-forget: if replicas exist we issue one UpdateTable to remove them and return
  // `false` — replica removal is asynchronous on the AWS side (can take minutes to tens of
  // minutes) and we don't want to stall the callers. A subsequent cleanup cycle will observe
  // the replicas are gone and issue DeleteTable. Returns true iff the primary was deleted this call.
  private[aws] def removeReplicasThenDelete(tableName: String, modeTag: Map[String, String] = Map.empty): Boolean = {
    val cleanupMetrics = metricsContext.withSuffix("cleanup")
    val physicalName = prefixedDynamoDbClient.prefixTableName(tableName)
    val replicaStatuses = fetchReplicaStatuses(tableName)
    if (replicaStatuses.nonEmpty) {
      // Only ACTIVE replicas can be removed via DeleteReplicationGroupMemberAction. Any other
      // state (DELETING = removal already in flight; CREATING / UPDATING / CREATION_FAILED /
      // REGION_DISABLED / INACCESSIBLE_ENCRYPTION_CREDENTIALS = transitional or terminal-bad)
      // makes AWS reject the delete action. In practice these are rare 30 days after table
      // creation, so we skip the whole table this cycle and surface a metric — a persistently
      // stuck replica is an operational problem in its own right, not something cleanup can fix.
      val active = replicaStatuses.collect { case (region, ReplicaStatus.ACTIVE) => region }.toSeq
      val nonActive = replicaStatuses.filterNot { case (_, s) => s == ReplicaStatus.ACTIVE }
      if (nonActive.nonEmpty) {
        logger.info(s"Table '$physicalName' has non-ACTIVE replicas ${nonActive
            .map { case (r, s) => s"$r=$s" }
            .toSeq
            .sorted
            .mkString(", ")}; skipping this cycle")
        cleanupMetrics.increment("skipped_non_active_replica", modeTag)
        return false
      }
      logger.info(s"Table '$physicalName' has replicas in ${active.sorted
          .mkString(", ")}; issuing async removal — DeleteTable deferred to a later cleanup cycle")
      val replicaUpdates = active.map { region =>
        ReplicationGroupUpdate
          .builder()
          .delete(DeleteReplicationGroupMemberAction.builder().regionName(region).build())
          .build()
      }
      val updateRequest = UpdateTableRequest
        .builder()
        .tableName(tableName)
        .replicaUpdates(replicaUpdates.toList.toJava)
        .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
        .build()
      prefixedDynamoDbClient.updateTable(updateRequest).join()
      cleanupMetrics.increment("replica_removal_issued", modeTag)
      return false
    }
    val deleteRequest = DeleteTableRequest
      .builder()
      .tableName(tableName)
      .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
      .build()
    prefixedDynamoDbClient.deleteTable(deleteRequest).join()
    true
  }

  // Paginate ListTables, filter by prefix / regex + age on the fly, stop as soon as we have
  // `maxDelete` deletion-eligible names.
  //
  // `prefixedDynamoDbClient.listTables` scopes results to this deployment's `tablePrefix`
  // automatically — cross-deployment tables never surface here. That means:
  //   - Scoped mode: page returns only in-deployment names; we further narrow to the logical
  //     dataset prefix and stop the moment names diverge past it.
  //   - Unscoped mode: page returns this deployment's tables in ASCII order; we filter each
  //     page by [[BatchTableNameRegex]] to skip non-batch-shaped names (e.g. streaming tables
  //     or non-Chronon tables that happen to share the deployment prefix).
  private[aws] def collectDeletionCandidates(prefix: Option[String], cutoff: LocalDate, maxDelete: Int): Seq[String] = {
    if (maxDelete <= 0) return Seq.empty
    // Use a modest page size — no need for the full 100 when we only want a handful, but not so
    // small that a prefix with mostly-recent tables burns roundtrips before finding old ones.
    val pageSize = math.min(100, math.max(20, maxDelete))
    val collected = mutable.ListBuffer.empty[String]
    val seen = mutable.Set.empty[String]
    var exclusiveStart: Option[String] = prefix.map(_.dropRight(1))
    var hasMore = true
    while (hasMore && collected.size < maxDelete) {
      val reqBuilder = ListTablesRequest.builder.limit(pageSize)
      exclusiveStart.foreach(reqBuilder.exclusiveStartTableName)
      val resp = prefixedDynamoDbClient.listTables(reqBuilder.build()).join()
      val page = resp.tableNames().toScala
      val (inScope, divergedPastPrefix) = prefix match {
        case Some(p) =>
          val matching = page.takeWhile(_.startsWith(p))
          (matching, matching.size < page.size)
        case None =>
          (page.filter(BatchTableNameRegex.pattern.matcher(_).matches()), false)
      }
      inScope.iterator
        .filterNot(seen.contains)
        .foreach { name =>
          seen += name
          parseBatchTableDate(name).foreach { date =>
            if (date.isBefore(cutoff) && collected.size < maxDelete) collected += name
          }
        }
      if (divergedPastPrefix || resp.lastEvaluatedTableName() == null) hasMore = false
      else exclusiveStart = Some(resp.lastEvaluatedTableName())
    }
    collected.toSeq
  }

  // Parses the {YYYY_MM_DD} date from a Chronon batch physical table name. Anchors on the
  // trailing {YYYY_MM_DD}_{epochMillis} tail (see [[BatchTableNameRegex]]) so scoped and
  // unscoped callers share one path — no need to know the logical prefix ahead of time.
  private def parseBatchTableDate(tableName: String): Option[LocalDate] = {
    BatchTableNameRegex.findFirstMatchIn(tableName).flatMap { m =>
      try Some(LocalDate.parse(m.group(1), BatchTableDateFormatter))
      catch {
        case e: DateTimeParseException =>
          logger.warn(s"Could not parse date from batch table name '$tableName': ${e.getMessage}")
          None
      }
    }
  }

  /** Converts a Hadoop Path to an S3BucketSource for DynamoDB ImportTable. */
  private def toS3BucketSource(path: org.apache.hadoop.fs.Path): S3BucketSource = {
    val uri = path.toUri
    S3BucketSource
      .builder()
      .s3Bucket(uri.getHost)
      .s3KeyPrefix(uri.getPath.stripPrefix("/") + "/")
      .build()
  }

  private[aws] def configuredImportTimeout: Duration =
    conf
      .get(KvUploadTimeoutMsKey)
      .orElse(conf.get(IonPathConfig.IonWriterTimeoutKey))
      .map(timeoutMillis => Duration.ofMillis(timeoutMillis.toLong))
      .getOrElse(DynamoImportDefaultTimeout)

  private def waitForImportCompletion(importArn: String, tableName: String, timeout: Duration): Unit = {
    val maxWaitTimeMs = timeout.toMillis
    val pollIntervalMs = 10 * 1000L // 10 seconds
    val startTime = System.currentTimeMillis()

    var status: ImportStatus = ImportStatus.IN_PROGRESS
    var lastDescription: ImportTableDescription = null
    while (status == ImportStatus.IN_PROGRESS && (System.currentTimeMillis() - startTime) < maxWaitTimeMs) {
      Thread.sleep(pollIntervalMs)

      try {
        val describeRequest = DescribeImportRequest
          .builder()
          .importArn(importArn)
          .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
          .build()
        val describeResponse = prefixedDynamoDbClient.describeImport(describeRequest).join()
        lastDescription = describeResponse.importTableDescription()
        status = lastDescription.importStatus()

        val elapsed = (System.currentTimeMillis() - startTime) / 1000
        logger.info(
          s"DynamoDB import status for $tableName: $status " +
            s"(${elapsed}s elapsed, processed: ${lastDescription.processedItemCount()} items, " +
            s"imported: ${lastDescription.importedItemCount()} items, " +
            s"errors: ${lastDescription.errorCount()})")
      } catch {
        case e: Exception =>
          logger.error(s"Error polling import status for $tableName", e)
          throw e
      }
    }

    status match {
      case ImportStatus.COMPLETED =>
        logger.info(
          s"DynamoDB import completed successfully for table: $tableName " +
            s"(imported: ${lastDescription.importedItemCount()} items, errors: ${lastDescription.errorCount()})")
      case ImportStatus.FAILED | ImportStatus.CANCELLED =>
        val diagnostics =
          s"""DynamoDB import failed for table: $tableName
             |  Status: $status
             |  Failure Code: ${lastDescription.failureCode()}
             |  Failure Message: ${lastDescription.failureMessage()}
             |  Error Count: ${lastDescription.errorCount()}
             |  Processed Items: ${lastDescription.processedItemCount()}
             |  Imported Items: ${lastDescription.importedItemCount()}
             |  Import ARN: $importArn""".stripMargin
        logger.error(diagnostics)
        throw new RuntimeException(diagnostics)
      case ImportStatus.IN_PROGRESS =>
        throw new RuntimeException(s"DynamoDB import timed out after $timeout for table: $tableName")
      case _ =>
        logger.warn(s"Unknown import status: $status for table: $tableName")
    }
  }

  protected def handleDynamoDbOperation[T](context: Context, dataset: String, startTs: Long)(
      completableFuture: CompletableFuture[T]): Future[T] = {
    FutureConverters.toScala(completableFuture).transform {
      case Success(result) =>
        context.distribution("latency", System.currentTimeMillis() - startTs)
        Success(result)
      case Failure(exception) =>
        exception match {
          case e: ProvisionedThroughputExceededException =>
            logger.error(s"Provisioned throughput exceeded as we are low on IOPS on $dataset", e)
            context.increment("iops_error")
            Failure(e)
          case e: ResourceNotFoundException =>
            logger.error(s"Unable to trigger operation on $dataset as its not found", e)
            context.increment("missing_table")
            Failure(e)
          case e: CompletionException =>
            e.getCause match {
              case ce: ProvisionedThroughputExceededException =>
                logger.error(s"Provisioned throughput exceeded as we are low on IOPS on $dataset", ce)
                context.increment("iops_error")
                Failure(ce)
              case ce: ResourceNotFoundException =>
                logger.error(s"Unable to trigger operation on $dataset as its not found", ce)
                context.increment("missing_table")
                Failure(ce)
              case _ =>
                logger.error("Error interacting with DynamoDB", e.getCause)
                context.increment("dynamodb_error")
                Failure(e.getCause)
            }
          case e: Exception =>
            logger.error("Error interacting with DynamoDB", e)
            context.increment("dynamodb_error")
            Failure(e)
        }
    }
  }

  protected def extractTimedValues(ddbResponseList: util.List[util.Map[String, AttributeValue]],
                                   defaultTimestamp: Long): Try[Seq[TimedValue]] = {
    Try {
      ddbResponseList.toScala.filterNot(_.isEmpty).map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala

        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())
        if (valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing valueBytes")

        val timestamp = responseMap.get(sortKeyColumn).map(v => v.n().toLong).getOrElse(defaultTimestamp)
        TimedValue(valueBytes.get, timestamp)
      }
    }
  }

  private def extractListValues(scanResponse: ScanResponse): Try[Seq[ListValue]] = {
    Try {
      scanResponse.items().toScala.filterNot(_.isEmpty).map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala

        val keyBytes = responseMap.get("keyBytes").map(v => v.b().asByteArray())
        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())

        if (keyBytes.isEmpty || valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing key / valueBytes")
        ListValue(keyBytes.get, valueBytes.get)
      }
    }
  }

  // Global Tables v2: called after a table is active to add read replicas in additional regions.
  // When `waitForActive` is true, blocks until every configured replica reports ACTIVE — required
  // for callers that immediately publish a pointer to the new physical table (e.g. bulkPut flipping
  // the batch-table registry). Non-atomic callers (create()) pass false and rely on eventual
  // consistency, avoiding multi-minute blocks on their thread.
  // Idempotent: pre-checks existing replicas via DescribeTable and only issues CreateReplication
  // for regions not already present (AWS rejects duplicate CreateReplicationGroupMemberAction with
  // a ValidationException). This makes retries safe when a first attempt created the table but
  // failed before replicas reached ACTIVE.
  protected def addReplicaRegions(tableName: String, waitForActive: Boolean): Unit = {
    if (replicaRegions.isEmpty) return

    val existingReplicas = fetchReplicaStatuses(tableName).keySet
    val regionsToCreate = replicaRegions.filterNot(existingReplicas.contains)

    if (regionsToCreate.nonEmpty) {
      val replicaUpdates = regionsToCreate.map { region =>
        ReplicationGroupUpdate
          .builder()
          .create(CreateReplicationGroupMemberAction.builder().regionName(region).build())
          .build()
      }
      val updateRequest = UpdateTableRequest
        .builder()
        .tableName(tableName)
        .replicaUpdates(replicaUpdates.toList.toJava)
        .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
        .build()
      prefixedDynamoDbClient.updateTable(updateRequest).join()
      logger.info(
        s"Global Table replica creation initiated for '$tableName' in regions: ${regionsToCreate.mkString(", ")}")
    } else {
      logger.info(
        s"All configured replicas already present for '$tableName' in regions: ${replicaRegions.mkString(", ")}")
    }

    if (waitForActive) {
      waitForReplicasActive(tableName, replicaRegions.toSet, configuredReplicaWaitTimeout)
    }
  }

  private[aws] def configuredReplicaWaitTimeout: Duration = {
    val timeout = conf
      .get(DynamoDbReplicaWaitTimeoutMsKey)
      .map(timeoutMillis => Duration.ofMillis(timeoutMillis.toLong))
      .getOrElse(DynamoReplicaWaitDefaultTimeout)
    // A single DescribeTable can take up to 30s (control-plane apiCallTimeout) and we poll every 15s;
    // require at least a minute so the caller's timeout budget can absorb one full poll cycle plus a
    // slow response without immediately tripping.
    require(
      timeout.compareTo(MinReplicaWaitTimeout) >= 0,
      s"$DynamoDbReplicaWaitTimeoutMsKey must be >= ${MinReplicaWaitTimeout.toMillis}ms; got ${timeout.toMillis}ms"
    )
    timeout
  }

  // Fetch current replica statuses from the source region's DescribeTable response.
  // Extracted as a seam so tests can stub replica progression without a real Global Table.
  private[aws] def fetchReplicaStatuses(tableName: String): Map[String, ReplicaStatus] = {
    val describeRequest = DescribeTableRequest.builder
      .tableName(tableName)
      .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
      .build
    val table = prefixedDynamoDbClient.describeTable(describeRequest).join().table()
    Option(table.replicas()).map(_.toScala).getOrElse(Seq.empty).map(r => r.regionName() -> r.replicaStatus()).toMap
  }

  // Poll DescribeTable in the source region until every expected replica is ACTIVE.
  // Throws if any replica enters CREATION_FAILED or if the wait exceeds `timeout`.
  private[aws] def waitForReplicasActive(tableName: String, expectedRegions: Set[String], timeout: Duration): Unit = {
    val maxWaitTimeMs = timeout.toMillis
    val startTime = System.currentTimeMillis()

    while (true) {
      val statusByRegion = fetchReplicaStatuses(tableName)

      val failed = statusByRegion.collect {
        case (region, ReplicaStatus.CREATION_FAILED) if expectedRegions.contains(region) => region
      }
      if (failed.nonEmpty) {
        throw new RuntimeException(
          s"Replica creation failed for table '$tableName' in regions: ${failed.mkString(", ")}")
      }

      val pending = expectedRegions.filterNot(r => statusByRegion.get(r).contains(ReplicaStatus.ACTIVE))
      if (pending.isEmpty) {
        logger.info(
          s"Global Table replicas ACTIVE for '$tableName' in regions: ${expectedRegions.toSeq.sorted.mkString(", ")}")
        return
      }

      val elapsedMs = System.currentTimeMillis() - startTime
      if (elapsedMs >= maxWaitTimeMs) {
        val statusStr = expectedRegions.toSeq.sorted
          .map(r => s"$r=${statusByRegion.getOrElse(r, "NOT_PRESENT")}")
          .mkString(", ")
        throw new RuntimeException(
          s"Timed out after ${timeout} waiting for replicas ACTIVE for table '$tableName'. Statuses: $statusStr")
      }

      logger.info(
        s"Waiting for replicas ACTIVE for '$tableName' (${elapsedMs / 1000}s elapsed); pending: ${pending.toSeq.sorted
            .mkString(", ")}")
      // Cap the sleep at the remaining wait budget so we don't overshoot the configured timeout
      // (e.g. avoid sleeping 15s when only 2s remain).
      val remainingMs = math.max(0L, maxWaitTimeMs - elapsedMs)
      Thread.sleep(math.min(replicaWaitPollIntervalMs, remainingMs))
    }
  }

  protected def replicaWaitPollIntervalMs: Long = ReplicaWaitPollIntervalMs
}

// One shared executor keeps discarded-but-not-closed instances (e.g. from genMetricsKvStore /
// genEnhancedStatsKvStore, which build a fresh KVStore per call) from each leaking their own
// scheduler thread.
private[aws] object BatchRegistryScheduler {
  type Handle = ScheduledFuture[_]

  // Lazy so callers that never enable scheduled refresh (interval <= 0) don't spin up a thread.
  // setRemoveOnCancelPolicy(true) so unregister() promptly frees the queue slot instead of waiting
  // for the next scheduled fire — matters when tests churn through many short-lived registrations.
  private lazy val executor: ScheduledThreadPoolExecutor = {
    val counter = new AtomicInteger(0)
    val exec = new ScheduledThreadPoolExecutor(
      1,
      (r: Runnable) => {
        val t = new Thread(r)
        t.setDaemon(true)
        t.setName(s"chronon-dynamo-batch-registry-refresh-${counter.incrementAndGet()}")
        t
      }
    )
    exec.setRemoveOnCancelPolicy(true)
    exec
  }

  def register(intervalMs: Long, callback: () => Unit, onError: Throwable => Unit): Handle = {
    require(intervalMs > 0, s"intervalMs must be > 0; got $intervalMs")
    executor.scheduleWithFixedDelay(
      () =>
        try callback()
        catch {
          case e: Exception =>
            try onError(e)
            catch { case _: Throwable => () }
        },
      intervalMs,
      intervalMs,
      TimeUnit.MILLISECONDS
    )
  }

  def unregister(handle: Handle): Unit = {
    handle.cancel(false)
    ()
  }

  private[aws] def activeRegistrations: Int = executor.getQueue.size()
}

object DynamoDBKVStoreConstants {
  val batchTableRegistry: String = "CHRONON_BATCH_TABLE_REGISTRY"
  val batchSuffix = "_BATCH"

  // Optional field that indicates if this table is meant to be time sorted in Dynamo or not
  val isTimedSorted = "is-time-sorted"

  // Name of the partition key column to use
  val partitionKeyColumn = "keyBytes"

  // Name of the time sort key column to use
  val sortKeyColumn = Constants.TimeColumn

  // Streaming tables use TileKey wrapping for tiled data.
  def isStreamingTable(dataset: String): Boolean = dataset.endsWith("_STREAMING")

  // Control plane operations (ImportTable, DeleteTable, DescribeImport) are slower than
  // data plane operations (GetItem, PutItem). Use higher timeouts to avoid intermittent failures.
  val ControlPlaneApiOverride: AwsRequestOverrideConfiguration = AwsRequestOverrideConfiguration
    .builder()
    .apiCallTimeout(Duration.ofSeconds(30))
    .apiCallAttemptTimeout(Duration.ofSeconds(10))
    .build()

  case class TileKeyComponents(baseKeyBytes: Array[Byte], tileSizeMillis: Long, tileStartTimestampMillis: Long)

  /** Unwraps a TileKey to extract the entity key for use as DynamoDB partition key.
    *
    * Streaming tables have two serialization layers:
    *   - Outer: Thrift (TileKey struct with dataset, keyBytes, tileSizeMs, tileStartTs)
    *   - Inner: Avro (entity key, e.g. customer_id, stored in TileKey.keyBytes)
    *
    * This method deserializes only the Thrift layer. The returned baseKeyBytes
    * remain Avro-encoded and are used directly as the DynamoDB partition key.
    */
  def extractTileKeyComponents(keyBytes: Array[Byte]): TileKeyComponents = {
    val tileKey = TilingUtils.deserializeTileKey(keyBytes)
    val baseKeyBytes = tileKey.keyBytes.toScala.map(_.toByte).toArray
    val tileSizeMs = tileKey.tileSizeMillis
    val tileStartTs = tileKey.tileStartTimestampMillis
    TileKeyComponents(baseKeyBytes, tileSizeMs, tileStartTs)
  }

  val DataTTLSeconds = 5.days.toSeconds.toInt
  val MillisPerDay = 1.day.toMillis
  val DynamoImportDefaultTimeout: Duration = Duration.ofMinutes(60)
  val DynamoReplicaWaitDefaultTimeout: Duration = Duration.ofMinutes(60)
  val MinReplicaWaitTimeout: Duration = Duration.ofMinutes(1)
  val ReplicaWaitPollIntervalMs: Long = 15 * 1000L
  // Off by default; opt-in via DYNAMO_BATCH_REGISTRY_REFRESH_INTERVAL_MS. When off, cache freshness
  // is driven by the request-path refresh() fallback at BatchTableCacheRefreshIntervalWhenSchedulerDisabledMs.
  val DefaultBatchRegistryRefreshIntervalMs: Long = 0L
  // Matches TTLCache's pre-scheduler default refresh cadence. Used when the scheduled refresher is
  // disabled so request-driven refresh() still catches registry swaps at ~8s granularity.
  val BatchTableCacheRefreshIntervalWhenSchedulerDisabledMs: Long = 8 * 1000L

  // DynamoDB BatchGetItem hard limit: max 100 keys per request. Not exposed as a
  // public constant by the AWS SDK v2, so we mirror it here.
  val BatchGetItemMaxKeys: Int = 100

  val DynamoEnableBatchGetKey: String = "DYNAMO_ENABLE_BATCH_GET"

  def getOptional(key: String, conf: Map[String, String]): Option[String] =
    sys.env.get(key).orElse(conf.get(key))

  // Array[Byte] doesn't implement structural equality; wrap it so keys can dedupe in a Map/Set.
  final class KeyWrapper(val bytes: Array[Byte]) {
    private val hash: Int = util.Arrays.hashCode(bytes)
    override def hashCode(): Int = hash
    override def equals(other: Any): Boolean = other match {
      case that: KeyWrapper => util.Arrays.equals(bytes, that.bytes)
      case _                => false
    }
  }
  object KeyWrapper {
    def apply(bytes: Array[Byte]): KeyWrapper = new KeyWrapper(bytes)
  }

  // Per-request state carried through the batch pipeline: original request, resolved physical
  // table, and the wrapped key used for map lookups. Kept in caller-request order so the final
  // Seq[Future[GetResponse]] preserves that order.
  final case class ResolvedLookup(req: KVStore.GetRequest, table: String, key: KeyWrapper)
  object ResolvedLookup {
    def apply(req: KVStore.GetRequest, resolveTable: String => String): ResolvedLookup =
      ResolvedLookup(req, resolveTable(req.dataset), KeyWrapper(req.keyBytes))
  }

  // Per-key outcome within a successfully-completed batch chunk. Split out from Try so the
  // UnprocessedKey Failure message can name the caller's logical dataset rather than the
  // resolved physical table (only known at assembleResponse time).
  sealed trait ChunkKeyOutcome
  object ChunkKeyOutcome {
    final case class Item(value: util.Map[String, AttributeValue]) extends ChunkKeyOutcome
    case object Unprocessed extends ChunkKeyOutcome
  }

  val BatchTableGCAgeDays = 30
  val BatchTableGCMaxDelete = 10
  // sweep delete counts are lower as we run them periodically
  val BatchTableSweepMaxDelete = 5
  // Batch table names embed the date with '_' separators (e.g. 2026_04_16) since '-' is not valid in DynamoDB table names
  val BatchTableDateFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern(PartitionSpec.daily.format.replace("-", "_"))
  // Chronon batch physical table name shapes (see bulkPut):
  //   Daily:  {SANITIZED_LOGICAL}_YYYY_MM_DD_{epochMillis}
  //   Hourly: {SANITIZED_LOGICAL}_YYYY_MM_DD_HH_00_{epochMillis} (hourly partition spec)
  // We anchor on the trailing 10+ digit epoch to keep false-positive risk low against arbitrary
  // non-Chronon tables in the same account.
  val BatchTableNameRegex: scala.util.matching.Regex =
    """.*_(\d{4}_\d{2}_\d{2})(?:_\d{2}_\d{2})?_\d{10,}$""".r

  def roundToDay(timestampMillis: Long): Long = {
    timestampMillis - (timestampMillis % MillisPerDay)
  }

  // Partition key format: {entity-key}#{dayTs}#{tileSizeMs}
  def buildKeyWithTileSize(baseKeyBytes: Array[Byte], timestampMillis: Long, tileSizeMs: Long): Array[Byte] = {
    val dayTs = roundToDay(timestampMillis)
    baseKeyBytes ++ s"#$dayTs#$tileSizeMs".getBytes(Charset.forName("UTF-8"))
  }

  def generateTimeSeriesKeys(baseKeyBytes: Array[Byte],
                             startTs: Long,
                             endTs: Long,
                             tileSizeMs: Long): Seq[Array[Byte]] = {
    val startDay = roundToDay(startTs)
    val endDay = roundToDay(endTs)
    (startDay to endDay by MillisPerDay).map { dayTs =>
      buildKeyWithTileSize(baseKeyBytes, dayTs, tileSizeMs)
    }
  }

  def primaryKeyMap(keyBytes: Array[Byte]): Map[String, AttributeValue] = {
    Map(partitionKeyColumn -> AttributeValue.builder.b(SdkBytes.fromByteArray(keyBytes)).build)
  }

  def buildAttributeMap(keyBytes: Array[Byte], valueBytes: Array[Byte]): Map[String, AttributeValue] = {
    primaryKeyMap(keyBytes) ++
      Map(
        "valueBytes" -> AttributeValue.builder.b(SdkBytes.fromByteArray(valueBytes)).build
      )
  }

  // Builds a DynamoDB query for a partition key with a time range on the sort key.
  def buildTimeRangeQuery(dataset: String,
                          partitionKeyBytes: Array[Byte],
                          startTs: Long,
                          endTs: Option[Long]): QueryRequest = {
    val partitionAlias = "#pk"
    val timeAlias = "#ts"
    val attrNameAliasMap = Map(partitionAlias -> partitionKeyColumn, timeAlias -> sortKeyColumn)
    val endTsResolved = endTs.getOrElse(System.currentTimeMillis())
    val attrValuesMap = Map(
      ":partitionKeyValue" -> AttributeValue.builder.b(SdkBytes.fromByteArray(partitionKeyBytes)).build,
      ":start" -> AttributeValue.builder.n(startTs.toString).build,
      ":end" -> AttributeValue.builder.n(endTsResolved.toString).build
    )

    QueryRequest.builder
      .tableName(dataset)
      .keyConditionExpression(s"$partitionAlias = :partitionKeyValue AND $timeAlias BETWEEN :start AND :end")
      .expressionAttributeNames(attrNameAliasMap.toJava)
      .expressionAttributeValues(attrValuesMap.toJava)
      .build
  }

  // Queries by partition key only — used for time-sorted tables when the sort key is unknown,
  // e.g. schema/metadata rows whose write timestamp isn't tracked at read time.
  def buildPartitionOnlyQuery(dataset: String, partitionKeyBytes: Array[Byte]): QueryRequest = {
    val partitionAlias = "#pk"
    QueryRequest.builder
      .tableName(dataset)
      .keyConditionExpression(s"$partitionAlias = :partitionKeyValue")
      .expressionAttributeNames(Map(partitionAlias -> partitionKeyColumn).toJava)
      .expressionAttributeValues(
        Map(":partitionKeyValue" -> AttributeValue.builder.b(SdkBytes.fromByteArray(partitionKeyBytes)).build).toJava)
      .build
  }
}
