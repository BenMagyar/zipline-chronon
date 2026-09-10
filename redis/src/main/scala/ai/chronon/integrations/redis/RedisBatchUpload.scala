package ai.chronon.integrations.redis

import redis.clients.jedis.JedisCluster
import redis.clients.jedis.exceptions.{JedisDataException, JedisNoScriptException}
import redis.clients.jedis.params.SetParams

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Wire formats and atomic Redis operations used by incremental batch uploads.
  *
  * This object is deliberately small and Redis-focused: it does not decide which keys changed, which generation won,
  * or when recovery is needed. Those decisions live in the Spark/S3 loader. This object provides the binary records
  * and Lua scripts that make the final Redis mutations monotonic and replayable.
  *
  * High-level publication flow:
  *
  * {{{
  *   flowchart TD
  *       catalog["Catalog snapshot"]
  *       diff["Spark diff against current S3 state"]
  *       generation["Immutable candidate generation"]
  *       state["state: full active key + value + digest + last value tuple + bucket"]
  *       delta["delta: changed value or tombstone + batch tuple + bucket"]
  *       tombstones["tombstones: TTL-bounded deletes retained for older-snapshot recovery"]
  *       records["_CANDIDATE -> _READY -> _APPLIED"]
  *       lease["mutable _LEASE minimum entity deadline"]
  *       head["Durable _HEAD"]
  *       redis["Pipelined, fenced Redis mutations"]
  *       metadata["serving metadata"]
  *       status["Redis status"]
  *
  *       catalog --> diff --> generation
  *       generation --> state
  *       generation --> delta
  *       generation --> tombstones
  *       generation --> records
  *       generation --> lease
  *       generation -- S3 If-Match CAS --> head
  *       head --> redis
  *       redis --> metadata
  *       metadata --> lease --> status
  * }}}
  *
  * Redis stores serving values and a recoverable status mirror. S3 remains the durable authority for generation
  * lineage, so losing Redis status or partially applying a generation can be repaired from the immutable, value-bearing
  * S3 generation without depending on a mutable catalog partition.
  *
  * Per-key Redis values carry enough metadata to make a single-key decision without consulting S3. The first timestamp
  * is compatible with Chronon's existing Redis value prefix; the incremental path adds magic/version, operation, and
  * S3 epoch fields before the user payload. TTL is Redis key metadata, not part of the stored value bytes.
  *
  * {{{
  *   Redis value bytes
  *   +-------------------+----------------+---------+-----------+-------------+
  *   | source timestamp  | magic/version  | op code | S3 epoch  | user value  |
  *   | 8 bytes           | CRB2 / v1      | 1 byte  | 8 bytes   | payload     |
  *   +-------------------+----------------+---------+-----------+-------------+
  *
  *   op = UPSERT: payload is the encoded Chronon value
  *   op = DELETE: payload is empty and the value is a tombstone
  * }}}
  *
  * Mutation ordering:
  *
  * {{{
  *   incoming tuple = (source timestamp, S3 epoch, encoded bytes)
  *
  *   current timestamp > incoming timestamp       => reject stale write
  *   current timestamp < incoming timestamp       => apply newer write
  *   same timestamp, current epoch > incoming     => reject stale replay
  *   same timestamp, current epoch < incoming     => apply newer generation
  *   same timestamp and epoch, same bytes         => idempotent replay; refresh TTL
  *   same timestamp and epoch, different bytes    => hard error
  * }}}
  *
  * Rate limiting counts Redis key commands, not source records or pipeline flushes:
  *
  * {{{
  *   changed key   -> 1 Lua mutation command
  *   removed key   -> 1 Lua tombstone command
  *   unchanged key -> 1 expiration-fence check, or one legacy header-validation command during migration
  *   retained delete -> 1 Lua tombstone replay command
  *   serving metadata -> 1 final Lua mutation command
  *
  *   batch_size mutation/refresh commands are grouped into one pipeline flush, but the limiter reserves capacity
  *   for each command in the batch. Status and preflight commands are outside this data-command ceiling.
  * }}}
  */
object RedisBatchUpload {
  private val StatusMagic = 0x43524253 // CRBS
  private val StatusVersion = 1
  private val ValueMagic = Array[Byte]('C'.toByte, 'R'.toByte, 'B'.toByte, '2'.toByte)
  private val ValueVersion: Byte = 1
  private val ValueHeaderLength = 8 + ValueMagic.length + 1 + 1 + 8

  val UpsertOperation = "UPSERT"
  val DeleteOperation = "DELETE"

  sealed trait Operation extends Product with Serializable {
    def name: String
    def code: Byte
  }

  case object Upsert extends Operation {
    override val name: String = UpsertOperation
    override val code: Byte = 1
  }

  case object Delete extends Operation {
    override val name: String = DeleteOperation
    override val code: Byte = 2
  }

  object Operation {
    def fromName(name: String): Operation = name match {
      case UpsertOperation => Upsert
      case DeleteOperation => Delete
      case other           => throw new IllegalArgumentException(s"Unknown Redis batch operation: $other")
    }

    private[redis] def fromCode(code: Byte): Operation = code match {
      case Upsert.code => Upsert
      case Delete.code => Delete
      case other       => throw new IllegalArgumentException(s"Unknown Redis batch operation code: $other")
    }
  }

  case class BatchStatus(generation: String,
                         sourcePartition: String,
                         batchTimestamp: Long,
                         writeEpoch: Long,
                         retired: Boolean = false) {
    require(isSafeIdentifier(generation), s"Invalid Redis batch generation: $generation")
    require(sourcePartition != null && sourcePartition.nonEmpty, "Redis batch source partition must be non-empty")
    require(batchTimestamp >= 0, s"Redis batch timestamp must be non-negative: $batchTimestamp")
    require(writeEpoch >= 0, s"Redis batch write epoch must be non-negative: $writeEpoch")
  }

  case class DecodedValue(operation: Operation,
                          payload: Array[Byte],
                          storedTimestamp: Long,
                          writeEpoch: Long,
                          legacy: Boolean)

  // Epoch millis and write epochs are below Lua's exact-integer ceiling. Timestamp is the primary ordering key so a
  // delayed older source partition cannot win merely because it was retried later. Equal epochs are already applied.
  private val PutBaseIfNewerScript: Array[Byte] =
    """
      |local function numberAt(value, startIndex)
      |  local result = 0
      |  for i = startIndex, startIndex + 7 do result = result * 256 + string.byte(value, i) end
      |  return result
      |end
      |local function timestamp(value)
      |  if not value or string.len(value) < 8 then return -1 end
      |  return numberAt(value, 1)
      |end
      |local function writeEpoch(value)
      |  if not value or string.len(value) < 22 then return 0 end
      |  if string.sub(value, 9, 12) ~= 'CRB2' or string.byte(value, 13) ~= 1 then return 0 end
      |  local operation = string.byte(value, 14)
      |  if operation ~= 1 and operation ~= 2 then return 0 end
      |  return numberAt(value, 15)
      |end
      |local function nowMillis()
      |  local parts = redis.call('TIME')
      |  return tonumber(parts[1]) * 1000 + math.floor(tonumber(parts[2]) / 1000)
      |end
      |local function extendExpiry(key, expiresAt, now)
      |  local ttl = redis.call('PTTL', key)
      |  if ttl < 0 or expiresAt > now + ttl then redis.call('PEXPIREAT', key, expiresAt) end
      |end
      |local incomingTimestamp = tonumber(ARGV[1])
      |local incomingEpoch = tonumber(ARGV[2])
      |local expiresAt = tonumber(ARGV[4])
      |local allowExpired = tonumber(ARGV[5]) == 1
      |local currentHeader = redis.call('GETRANGE', KEYS[1], 0, 21)
      |if string.len(currentHeader) > 0 then
      |  local currentTimestamp = timestamp(currentHeader)
      |  if currentTimestamp > incomingTimestamp then return 0 end
      |  if currentTimestamp == incomingTimestamp then
      |    local currentEpoch = writeEpoch(currentHeader)
      |    if currentEpoch > incomingEpoch then return 0 end
      |    if currentEpoch == incomingEpoch then
      |      local current = redis.call('GET', KEYS[1])
      |      if current == ARGV[3] then
      |        local now = nowMillis()
      |        if not expiresAt or expiresAt <= now then
      |          if allowExpired then
      |            redis.call('PEXPIREAT', KEYS[1], 0)
      |            return 1
      |          end
      |          return redis.error_reply('Chronon mutation lease expired')
      |        end
      |        extendExpiry(KEYS[1], expiresAt, now)
      |        return 2
      |      end
      |      return redis.error_reply('Chronon conflicting mutation for identical timestamp and epoch')
      |    end
      |  end
      |end
      |if not expiresAt or expiresAt <= nowMillis() then
      |  if allowExpired then
      |    if string.len(currentHeader) > 0 then
      |      redis.call('PEXPIREAT', KEYS[1], 0)
      |      return 1
      |    end
      |    return 2
      |  end
      |  return redis.error_reply('Chronon mutation lease expired')
      |end
      |redis.call('SET', KEYS[1], ARGV[3], 'PXAT', expiresAt)
      |return 1
      |""".stripMargin.getBytes(StandardCharsets.UTF_8)

  // Read only the ordering header for unchanged values. A timestamp/epoch tuple identifies one immutable protocol
  // mutation, so renewing a matching UPSERT does not need to load and hash the full payload from a tiered value.
  // Returning 0 asks the loader to replay S3 state; returning 2 means a newer generation owns the key.
  private val ValidateAndExpireScript: Array[Byte] =
    """
      |local function numberAt(value, startIndex)
      |  local result = 0
      |  for i = startIndex, startIndex + 7 do result = result * 256 + string.byte(value, i) end
      |  return result
      |end
      |local expectedTimestamp = tonumber(ARGV[1])
      |local expectedEpoch = tonumber(ARGV[2])
      |local expiresAt = tonumber(ARGV[3])
      |local current = redis.call('GETRANGE', KEYS[1], 0, 21)
      |if string.len(current) < 8 then return 0 end
      |local currentTimestamp = numberAt(current, 1)
      |local currentEpoch = 0
      |local operation = string.byte(current, 14)
      |local versioned = string.len(current) >= 22 and string.sub(current, 9, 12) == 'CRB2' and string.byte(current, 13) == 1 and (operation == 1 or operation == 2)
      |if versioned then currentEpoch = numberAt(current, 15) end
      |if currentTimestamp > expectedTimestamp then return 2 end
      |if currentTimestamp < expectedTimestamp then return 0 end
      |if currentEpoch > expectedEpoch then return 2 end
      |if currentEpoch < expectedEpoch then return 0 end
      |if not versioned or operation ~= 1 then
      |  return redis.error_reply('Chronon active value has an unexpected wire format')
      |end
      |local parts = redis.call('TIME')
      |local now = tonumber(parts[1]) * 1000 + math.floor(tonumber(parts[2]) / 1000)
      |if not expiresAt or expiresAt <= now then
      |  return redis.error_reply('Chronon mutation lease expired')
      |end
      |local ttl = redis.call('PTTL', KEYS[1])
      |if ttl < 0 or expiresAt > now + ttl then redis.call('PEXPIREAT', KEYS[1], expiresAt) end
      |return 1
      |""".stripMargin.getBytes(StandardCharsets.UTF_8)

  // Once a generation has a durable lease checkpoint, its absolute expiration is also a per-key rollback fence.
  // Checking and advancing that metadata avoids reading a tiered value on the normal path. A later-than-checkpoint
  // expiration is validated against the ordering header before it is accepted, covering concurrent older writers.
  private val ValidateExpiryAndAdvanceScript: Array[Byte] =
    """
      |local function numberAt(value, startIndex)
      |  local result = 0
      |  for i = startIndex, startIndex + 7 do result = result * 256 + string.byte(value, i) end
      |  return result
      |end
      |local expectedMinimum = tonumber(ARGV[1])
      |local desired = tonumber(ARGV[2])
      |local expectedTimestamp = tonumber(ARGV[3])
      |local expectedEpoch = tonumber(ARGV[4])
      |if not expectedMinimum or expectedMinimum <= 0 then
      |  return redis.error_reply('Chronon expected lease deadline must be positive')
      |end
      |if not desired or desired < expectedMinimum then
      |  return redis.error_reply('Chronon desired lease deadline precedes its expected deadline')
      |end
      |if not expectedTimestamp or expectedTimestamp < 0 or not expectedEpoch or expectedEpoch < 0 then
      |  return redis.error_reply('Chronon expected ordering tuple must be non-negative')
      |end
      |local parts = redis.call('TIME')
      |local now = tonumber(parts[1]) * 1000 + math.floor(tonumber(parts[2]) / 1000)
      |if desired <= now then return redis.error_reply('Chronon mutation lease expired') end
      |local observed = redis.call('PEXPIRETIME', KEYS[1])
      |if observed < expectedMinimum then return 0 end
      |if observed > expectedMinimum then
      |  local current = redis.call('GETRANGE', KEYS[1], 0, 21)
      |  if string.len(current) < 8 then return 0 end
      |  local currentTimestamp = numberAt(current, 1)
      |  local currentEpoch = 0
      |  local operation = string.byte(current, 14)
      |  local versioned = string.len(current) >= 22 and string.sub(current, 9, 12) == 'CRB2' and string.byte(current, 13) == 1 and (operation == 1 or operation == 2)
      |  if versioned then currentEpoch = numberAt(current, 15) end
      |  if currentTimestamp > expectedTimestamp then return 2 end
      |  if currentTimestamp < expectedTimestamp then return 0 end
      |  if currentEpoch > expectedEpoch then return 2 end
      |  if currentEpoch < expectedEpoch then return 0 end
      |  if not versioned or operation ~= 1 then
      |    return redis.error_reply('Chronon active value has an unexpected wire format')
      |  end
      |end
      |if desired > observed then redis.call('PEXPIREAT', KEYS[1], desired) end
      |return 1
      |""".stripMargin.getBytes(StandardCharsets.UTF_8)

  private val SetStatusIfNewerScript: Array[Byte] =
    """#!lua flags=allow-oom
      |local function numberAt(value, startIndex)
      |  local result = 0
      |  for i = startIndex, startIndex + 7 do result = result * 256 + string.byte(value, i) end
      |  return result
      |end
      |local incomingTimestamp = tonumber(ARGV[1])
      |local incomingEpoch = tonumber(ARGV[2])
      |local incomingRetired = tonumber(ARGV[3])
      |local expiresAt = tonumber(ARGV[5])
      |local current = redis.call('GET', KEYS[1])
      |if current then
      |  if string.len(current) < 22 or string.sub(current, 18, 21) ~= 'CRBS' then
      |    return redis.error_reply('Chronon batch status has an invalid wire format')
      |  end
      |  local currentTimestamp = numberAt(current, 1)
      |  local currentEpoch = numberAt(current, 9)
      |  local currentRetired = string.byte(current, 17)
      |  if currentRetired == 1 and incomingRetired == 0 then
      |    return 0
      |  end
      |  if currentTimestamp > incomingTimestamp then return 0 end
      |  if currentTimestamp == incomingTimestamp then
      |    if currentEpoch > incomingEpoch then return 0 end
      |    if currentEpoch == incomingEpoch then
      |      if current == ARGV[4] then
      |        if expiresAt and expiresAt > 0 then
      |          local parts = redis.call('TIME')
      |          local now = tonumber(parts[1]) * 1000 + math.floor(tonumber(parts[2]) / 1000)
      |          if expiresAt <= now then return redis.error_reply('Chronon status lease expired') end
      |          local ttl = redis.call('PTTL', KEYS[1])
      |          if ttl < 0 or expiresAt > now + ttl then redis.call('PEXPIREAT', KEYS[1], expiresAt) end
      |        else
      |          redis.call('PERSIST', KEYS[1])
      |        end
      |        return 2
      |      end
      |      return redis.error_reply('Chronon conflicting batch status for identical timestamp and epoch')
      |    end
      |  end
      |end
      |if expiresAt and expiresAt > 0 then
      |  local parts = redis.call('TIME')
      |  local now = tonumber(parts[1]) * 1000 + math.floor(tonumber(parts[2]) / 1000)
      |  if expiresAt <= now then return redis.error_reply('Chronon status lease expired') end
      |  redis.call('SET', KEYS[1], ARGV[4], 'PXAT', expiresAt)
      |else
      |  redis.call('SET', KEYS[1], ARGV[4])
      |end
      |return 1
      |""".stripMargin.getBytes(StandardCharsets.UTF_8)

  // Incremental publication already requires Lua and must be able to establish its bounded control marker when a
  // noeviction cluster is at maxmemory. Full-snapshot publication uses ordinary GET/SET commands below so opting out
  // of the incremental protocol does not add a scripting ACL requirement.
  private val ClaimIncrementalPublicationModeScript: Array[Byte] =
    """#!lua flags=allow-oom
      |local current = redis.call('GET', KEYS[1])
      |if current then
      |  if current == ARGV[1] then return 2 end
      |  return redis.error_reply('Chronon Redis publication mode conflicts with the existing mode')
      |end
      |redis.call('SET', KEYS[1], ARGV[1])
      |return 1
      |""".stripMargin.getBytes(StandardCharsets.UTF_8)

  // Reserve one position on a cluster-wide virtual timeline. A reservation counts attempted key commands, not
  // pipeline requests. The configured burst allows at most one full pipeline batch immediately after an idle period.
  // Status and limiter state are bounded control-plane records and must remain writable above maxmemory so a retired
  // generation can reach its allocation-free UNLINK operations and release capacity.
  private val ReserveRateScript: Array[Byte] =
    """#!lua flags=allow-oom
      |local nowParts = redis.call('TIME')
      |local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)
      |local requestedCount = tonumber(ARGV[1])
      |local requestedRate = tonumber(ARGV[2])
      |local requestedBurst = tonumber(ARGV[3])
      |if requestedCount > requestedBurst then
      |  return redis.error_reply('Chronon rate request exceeds configured burst')
      |end
      |local configuredRate = tonumber(redis.call('HGET', KEYS[1], 'rate'))
      |local next = tonumber(redis.call('HGET', KEYS[1], 'next'))
      |if configuredRate and configuredRate ~= requestedRate and next and next > now then
      |  return redis.error_reply('Chronon active rate limiter has a different max keys per second')
      |end
      |local burstDuration = requestedBurst * 1000 / requestedRate
      |local requestDuration = requestedCount * 1000 / requestedRate
      |local earliest = now - burstDuration
      |next = next or earliest
      |next = math.max(next, earliest)
      |local readyAt = next + requestDuration
      |local wait = math.max(0, math.ceil(readyAt - now))
      |redis.call('HSET', KEYS[1], 'rate', requestedRate, 'next', readyAt)
      |local retention = math.max(60000, math.ceil(burstDuration * 4))
      |redis.call('PEXPIRE', KEYS[1], math.ceil(math.max(0, readyAt - now) + retention))
      |return wait
      |""".stripMargin.getBytes(StandardCharsets.UTF_8)

  private val PutBaseIfNewerSha = scriptSha(PutBaseIfNewerScript)
  private val ValidateAndExpireSha = scriptSha(ValidateAndExpireScript)
  private val ValidateExpiryAndAdvanceSha = scriptSha(ValidateExpiryAndAdvanceScript)
  private val SetStatusIfNewerSha = scriptSha(SetStatusIfNewerScript)
  private val ClaimIncrementalPublicationModeSha = scriptSha(ClaimIncrementalPublicationModeScript)
  private val ReserveRateSha = scriptSha(ReserveRateScript)

  def encodeStatus(status: BatchStatus): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    try {
      out.writeLong(status.batchTimestamp)
      out.writeLong(status.writeEpoch)
      out.writeBoolean(status.retired)
      out.writeInt(StatusMagic)
      out.writeByte(StatusVersion)
      out.writeUTF(status.generation)
      out.writeUTF(status.sourcePartition)
      out.flush()
      bytes.toByteArray
    } finally {
      out.close()
    }
  }

  def decodeStatus(bytes: Array[Byte]): Try[BatchStatus] = Try {
    require(bytes != null, "Redis batch status is null")
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      val batchTimestamp = in.readLong()
      val writeEpoch = in.readLong()
      val retired = in.readBoolean()
      require(in.readInt() == StatusMagic, "Redis batch status has an invalid magic header")
      val version = in.readUnsignedByte()
      require(version == StatusVersion, s"Unsupported Redis batch status version: $version")
      val status = BatchStatus(
        generation = in.readUTF(),
        sourcePartition = in.readUTF(),
        batchTimestamp = batchTimestamp,
        writeEpoch = writeEpoch,
        retired = retired
      )
      require(in.available() == 0, "Redis batch status has trailing bytes")
      status
    } finally {
      in.close()
    }
  }

  def encodeValue(operation: Operation,
                  batchTimestamp: Long,
                  payload: Array[Byte],
                  writeEpoch: Long = 0L): Array[Byte] = {
    require(batchTimestamp >= 0, s"Redis batch timestamp must be non-negative: $batchTimestamp")
    require(writeEpoch >= 0, s"Redis batch write epoch must be non-negative: $writeEpoch")
    if (operation == Upsert) require(payload != null, "Redis batch upsert payload must be non-null")
    val safePayload = Option(payload).getOrElse(Array.emptyByteArray)
    val buffer = ByteBuffer.allocate(ValueHeaderLength + safePayload.length)
    buffer.putLong(batchTimestamp)
    buffer.put(ValueMagic)
    buffer.put(ValueVersion)
    buffer.put(operation.code)
    buffer.putLong(writeEpoch)
    buffer.put(safePayload)
    buffer.array()
  }

  /** Decodes both the generation-aware format and the original timestamp-prefixed value format. */
  def decodeValue(bytes: Array[Byte]): Try[DecodedValue] = Try {
    require(bytes != null && bytes.length >= 8, "Redis batch value must contain an 8-byte timestamp")
    val timestamp = ByteBuffer.wrap(bytes, 0, 8).getLong
    val versionOffset = 8 + ValueMagic.length
    val operationOffset = versionOffset + 1
    val versioned = bytes.length >= ValueHeaderLength &&
      ValueMagic.indices.forall(i => bytes(8 + i) == ValueMagic(i)) &&
      bytes(versionOffset) == ValueVersion &&
      (bytes(operationOffset) == Upsert.code || bytes(operationOffset) == Delete.code)
    if (versioned) {
      val operation = Operation.fromCode(bytes(operationOffset))
      val writeEpoch = ByteBuffer.wrap(bytes, 8 + ValueMagic.length + 2, 8).getLong
      DecodedValue(operation, bytes.drop(ValueHeaderLength), timestamp, writeEpoch, legacy = false)
    } else {
      DecodedValue(Upsert, bytes.drop(8), timestamp, writeEpoch = 0L, legacy = true)
    }
  }

  def readStatus(jedisCluster: JedisCluster, statusKey: String): Option[BatchStatus] =
    Option(jedisCluster.get(statusKey.getBytes(StandardCharsets.UTF_8))).map(bytes => decodeStatus(bytes).get)

  def readPublicationMode(jedisCluster: JedisCluster, publicationModeKey: String): Option[RedisBatchModeSelection] =
    Option(jedisCluster.get(publicationModeKey.getBytes(StandardCharsets.UTF_8)))
      .map(bytes => RedisBatchModeSelection.parse(new String(bytes, StandardCharsets.UTF_8)))

  /** Atomically records the dataset's publication protocol. Returns 1 for a new claim and 2 for an idempotent claim.
    * A conflicting claim fails, as does a full-snapshot claim for a dataset that already has incremental status.
    */
  def claimPublicationMode(jedisCluster: JedisCluster,
                           dataset: String,
                           keyPrefix: String,
                           mode: RedisBatchModeSelection): Long = {
    require(mode != null, "Redis publication mode must be non-null")
    val modeKey = buildPublicationModeKey(dataset, keyPrefix)
    mode match {
      case RedisBatchModeSelection.Incremental =>
        val result = evalScript(
          jedisCluster,
          ClaimIncrementalPublicationModeScript,
          ClaimIncrementalPublicationModeSha,
          List(modeKey.getBytes(StandardCharsets.UTF_8)),
          List(mode.configValue.getBytes(StandardCharsets.UTF_8))
        )
        result.asInstanceOf[java.lang.Long].longValue()
      case RedisBatchModeSelection.FullSnapshot =>
        val statusKey = buildStatusKey(dataset, keyPrefix)
        def requireNoIncrementalStatus(): Unit =
          if (jedisCluster.exists(statusKey))
            throw new JedisDataException(
              "Chronon full-snapshot publication cannot claim a dataset with incremental status")

        requireNoIncrementalStatus()
        val inserted = jedisCluster.set(modeKey, mode.configValue, SetParams.setParams().nx()) == "OK"
        if (!inserted) {
          val existing = Option(jedisCluster.get(modeKey))
          if (!existing.contains(mode.configValue))
            throw new JedisDataException("Chronon Redis publication mode conflicts with the existing mode")
        }
        // Check again so a pre-marker incremental writer cannot win between the compatibility check and the claim.
        requireNoIncrementalStatus()
        if (inserted) 1L else 2L
    }
  }

  /** Writes the recoverable Redis status mirror monotonically. Returns 1 for a write, 2 for exact replay, and 0 when
    * a strictly newer status already exists.
    */
  def writeStatus(jedisCluster: JedisCluster, statusKey: String, status: BatchStatus, ttlSeconds: Int = 0): Long = {
    require(ttlSeconds >= 0, s"Redis batch status TTL must be non-negative: $ttlSeconds")
    val expireAtMillis =
      if (ttlSeconds == 0) 0L
      else java.lang.Math.addExact(System.currentTimeMillis(), java.lang.Math.multiplyExact(ttlSeconds.toLong, 1000L))
    writeStatusUntil(jedisCluster, statusKey, status, expireAtMillis)
  }

  private[redis] def writeStatusUntil(jedisCluster: JedisCluster,
                                      statusKey: String,
                                      status: BatchStatus,
                                      expireAtMillis: Long): Long = {
    require(expireAtMillis >= 0, s"Redis batch status deadline must be non-negative: $expireAtMillis")
    val encoded = encodeStatus(status)
    val result = evalScript(
      jedisCluster,
      SetStatusIfNewerScript,
      SetStatusIfNewerSha,
      List(statusKey.getBytes(StandardCharsets.UTF_8)),
      List(
        status.batchTimestamp.toString.getBytes(StandardCharsets.UTF_8),
        status.writeEpoch.toString.getBytes(StandardCharsets.UTF_8),
        (if (status.retired) "1" else "0").getBytes(StandardCharsets.UTF_8),
        encoded,
        expireAtMillis.toString.getBytes(StandardCharsets.UTF_8)
      )
    )
    result.asInstanceOf[java.lang.Long].longValue()
  }

  private[redis] def putBaseIfNewerScript: Array[Byte] = PutBaseIfNewerScript
  private[redis] def putBaseIfNewerSha: Array[Byte] = PutBaseIfNewerSha

  private[redis] def putBaseIfNewer(jedisCluster: JedisCluster, key: Array[Byte], args: Seq[Array[Byte]]): Any =
    evalScript(jedisCluster, PutBaseIfNewerScript, PutBaseIfNewerSha, List(key), args)

  private[redis] def validateAndExpireSha: Array[Byte] = ValidateAndExpireSha

  private[redis] def validateAndExpire(jedisCluster: JedisCluster, key: Array[Byte], args: Seq[Array[Byte]]): Any =
    evalScript(jedisCluster, ValidateAndExpireScript, ValidateAndExpireSha, List(key), args)

  private[redis] def validateExpiryAndAdvanceSha: Array[Byte] = ValidateExpiryAndAdvanceSha

  private[redis] def validateExpiryAndAdvance(jedisCluster: JedisCluster,
                                              key: Array[Byte],
                                              args: Seq[Array[Byte]]): Any =
    evalScript(jedisCluster, ValidateExpiryAndAdvanceScript, ValidateExpiryAndAdvanceSha, List(key), args)

  /** Reserves cluster-wide write capacity and returns how long the caller must wait before using it. */
  def reserveRate(jedisCluster: JedisCluster,
                  limiterKey: String,
                  keyCount: Int,
                  maxKeysPerSecond: Int,
                  maxBurst: Int = -1): Long = {
    require(keyCount > 0, s"Rate reservation key count must be positive: $keyCount")
    require(maxKeysPerSecond > 0, s"Redis max keys per second must be positive: $maxKeysPerSecond")
    val effectiveBurst = if (maxBurst < 0) keyCount else maxBurst
    require(effectiveBurst > 0, s"Redis rate limiter burst must be positive: $effectiveBurst")
    val result = evalScript(
      jedisCluster,
      ReserveRateScript,
      ReserveRateSha,
      List(limiterKey.getBytes(StandardCharsets.UTF_8)),
      List(keyCount, maxKeysPerSecond, effectiveBurst).map(_.toString.getBytes(StandardCharsets.UTF_8))
    )
    result.asInstanceOf[java.lang.Long].longValue()
  }

  def isSafeIdentifier(value: String): Boolean =
    value != null && value.matches("[A-Za-z0-9._-]+")

  def buildStatusKey(dataset: String, keyPrefix: String): String =
    internalKey(dataset, "batch_status", keyPrefix)

  def buildPublicationModeKey(dataset: String, keyPrefix: String): String =
    internalKey(dataset, "publication_mode", keyPrefix)

  val RateLimiterKey = "{__chronon_batch_rate}"

  private def internalKey(dataset: String, name: String, keyPrefix: String): String = {
    val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix${RedisKVStoreConstants.KeySeparator}"
    s"$prefix{$dataset}:__chronon_$name"
  }

  private def evalScript(jedisCluster: JedisCluster,
                         script: Array[Byte],
                         sha: Array[Byte],
                         keys: Seq[Array[Byte]],
                         args: Seq[Array[Byte]]): Any = {
    require(keys.nonEmpty, "Redis Lua script requires at least one routing key")
    try jedisCluster.evalsha(sha, keys.asJava, args.asJava)
    catch {
      case _: JedisNoScriptException =>
        jedisCluster.scriptLoad(script, keys.head)
        jedisCluster.evalsha(sha, keys.asJava, args.asJava)
    }
  }

  private def scriptSha(script: Array[Byte]): Array[Byte] =
    MessageDigest
      .getInstance("SHA-1")
      .digest(script)
      .iterator
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
      .getBytes(StandardCharsets.US_ASCII)
}
