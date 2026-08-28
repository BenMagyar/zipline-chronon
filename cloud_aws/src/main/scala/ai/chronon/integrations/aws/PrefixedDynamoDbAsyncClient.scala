package ai.chronon.integrations.aws

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import java.util.concurrent.CompletableFuture

/** Wraps a DynamoDbAsyncClient to automatically prefix all table names.
  *
  * @param delegate the underlying DynamoDbAsyncClient to wrap
  * @param tablePrefix the prefix to apply to all table names
  */
class PrefixedDynamoDbAsyncClient(delegate: DynamoDbAsyncClient, tablePrefix: String = "") {

  private val logger = LoggerFactory.getLogger(getClass)

  private[aws] def prefixTableName(tableName: String): String = {
    if (tableName == null || tableName.isEmpty) tableName
    else tablePrefix + tableName
  }

  private def isTablePrefixed(tableName: String): Boolean = {
    tablePrefix.nonEmpty && tableName.startsWith(tablePrefix)
  }

  // ========== Supported Read Operations ==========

  def getItem(request: GetItemRequest): CompletableFuture[GetItemResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"getItem: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.getItem(prefixedRequest)
  }

  def batchGetItem(request: BatchGetItemRequest): CompletableFuture[BatchGetItemResponse] = {
    if (tablePrefix.isEmpty) {
      delegate.batchGetItem(request)
    } else {
      val originalItems = request.requestItems()
      val prefixedItems = new java.util.HashMap[String, KeysAndAttributes](originalItems.size())
      originalItems.forEach((tableName, keysAndAttrs) => prefixedItems.put(prefixTableName(tableName), keysAndAttrs))
      val prefixedRequest = request.toBuilder.requestItems(prefixedItems).build()
      delegate.batchGetItem(prefixedRequest).thenApply { response =>
        // Strip prefix from Responses and UnprocessedKeys so callers see logical table names
        val stripped = response.toBuilder
        if (response.hasResponses) {
          val newResponses = new java.util.HashMap[String, java.util.List[java.util.Map[String, AttributeValue]]]()
          response.responses().forEach((tableName, items) => newResponses.put(stripTableName(tableName), items))
          stripped.responses(newResponses)
        }
        if (response.hasUnprocessedKeys) {
          val newUnprocessed = new java.util.HashMap[String, KeysAndAttributes]()
          response.unprocessedKeys().forEach((tableName, ka) => newUnprocessed.put(stripTableName(tableName), ka))
          stripped.unprocessedKeys(newUnprocessed)
        }
        stripped.build()
      }
    }
  }

  private def stripTableName(name: String): String =
    if (isTablePrefixed(name)) name.substring(tablePrefix.length) else name

  def query(request: QueryRequest): CompletableFuture[QueryResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"query: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.query(prefixedRequest)
  }

  def scan(request: ScanRequest): CompletableFuture[ScanResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"scan: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.scan(prefixedRequest)
  }

  def describeTable(request: DescribeTableRequest): CompletableFuture[DescribeTableResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"describeTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.describeTable(prefixedRequest)
  }

  def describeImport(request: DescribeImportRequest): CompletableFuture[DescribeImportResponse] = {
    // DescribeImport uses ARN, not table name, so no prefixing needed
    delegate.describeImport(request)
  }

  // ========== Supported Write Operations ==========

  def putItem(request: PutItemRequest): CompletableFuture[PutItemResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"putItem: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.putItem(prefixedRequest)
  }

  def createTable(request: CreateTableRequest): CompletableFuture[CreateTableResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"createTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.createTable(prefixedRequest)
  }

  /** Pagination:
    *   - Callers pass a logical `exclusiveStartTableName` (or none for the first page). We
    *     compose the physical start key here. On page 1 with no cursor, we inject
    *     `tablePrefix` as the physical start so DynamoDB begins iterating at the prefix range
    *     (rather than the top of the account).
    *   - If a page's returned names diverge past `tablePrefix`, we truncate the response and
    *     suppress `lastEvaluatedTableName` — from the caller's perspective there are no more
    *     pages. This prevents an un-stripped other-deployment cursor from being fed back on
    *     the next page and re-prefixed by us into a phantom key that silently skips our
    *     tables (the old behavior).
    */
  def listTables(request: ListTablesRequest): CompletableFuture[ListTablesResponse] = {
    val hasPrefix = tablePrefix.nonEmpty
    val prefixedRequest = (request.exclusiveStartTableName(), hasPrefix) match {
      case (null, true)  => request.toBuilder.exclusiveStartTableName(tablePrefix).build()
      case (null, false) => request
      case (name, _)     => request.toBuilder.exclusiveStartTableName(prefixTableName(name)).build()
    }
    delegate.listTables(prefixedRequest).thenApply { response =>
      if (!hasPrefix) {
        response
      } else {
        val rawNames = response.tableNames()
        // takeWhile: results come back in ASCII order, so once a name diverges from the
        // prefix we're past our slice — everything after is other deployments.
        val inScopePhysical = rawNames.toArray(new Array[String](0)).takeWhile(_.startsWith(tablePrefix))
        val divergedPastPrefix = inScopePhysical.length < rawNames.size()
        val stripped = inScopePhysical.map(_.substring(tablePrefix.length))
        val builder = response.toBuilder.tableNames(stripped: _*)
        val lastEval = response.lastEvaluatedTableName()
        if (divergedPastPrefix) {
          // Explicitly clear — if we returned it, the caller would feed it back and we'd
          // re-prefix into a nonsense key. From their view, pagination is done.
          builder.lastEvaluatedTableName(null.asInstanceOf[String]).build()
        } else if (lastEval != null && lastEval.startsWith(tablePrefix)) {
          builder.lastEvaluatedTableName(lastEval.substring(tablePrefix.length)).build()
        } else {
          builder.build()
        }
      }
    }
  }

  def deleteTable(request: DeleteTableRequest): CompletableFuture[DeleteTableResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"deleteTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.deleteTable(prefixedRequest)
  }

  def updateTimeToLive(request: UpdateTimeToLiveRequest): CompletableFuture[UpdateTimeToLiveResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(
      s"updateTimeToLive: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.updateTimeToLive(prefixedRequest)
  }

  def updateTable(request: UpdateTableRequest): CompletableFuture[UpdateTableResponse] = {
    val originalTableName = request.tableName()
    val prefixedTableName = prefixTableName(originalTableName)
    logger.debug(s"updateTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
    val prefixedRequest = request.toBuilder.tableName(prefixedTableName).build()
    delegate.updateTable(prefixedRequest)
  }

  def importTable(request: ImportTableRequest): CompletableFuture[ImportTableResponse] = {
    // For ImportTableRequest, the table name is in tableCreationParameters
    val originalParams = request.tableCreationParameters()
    if (originalParams != null && originalParams.tableName() != null) {
      val originalTableName = originalParams.tableName()
      val prefixedTableName = prefixTableName(originalTableName)
      logger.debug(s"importTable: original table name='$originalTableName' -> prefixed table name='$prefixedTableName'")
      val prefixedParams = originalParams.toBuilder
        .tableName(prefixedTableName)
        .build()
      val prefixedRequest = request.toBuilder
        .tableCreationParameters(prefixedParams)
        .build()
      delegate.importTable(prefixedRequest)
    } else {
      delegate.importTable(request)
    }
  }

  // ========== Waiters ==========

  def waitUntilTableExists(tableName: String)
      : CompletableFuture[software.amazon.awssdk.core.waiters.WaiterResponse[DescribeTableResponse]] = {
    val prefixedTableName = prefixTableName(tableName)
    logger.debug(s"waitUntilTableExists: original table name='$tableName' -> prefixed table name='$prefixedTableName'")
    val request = DescribeTableRequest.builder.tableName(prefixedTableName).build
    delegate.waiter().waitUntilTableExists(request)
  }

  def waitUntilTableNotExists(tableName: String)
      : CompletableFuture[software.amazon.awssdk.core.waiters.WaiterResponse[DescribeTableResponse]] = {
    val prefixedTableName = prefixTableName(tableName)
    logger.debug(
      s"waitUntilTableNotExists: original table name='$tableName' -> prefixed table name='$prefixedTableName'")
    val request = DescribeTableRequest.builder.tableName(prefixedTableName).build
    delegate.waiter().waitUntilTableNotExists(request)
  }
}
