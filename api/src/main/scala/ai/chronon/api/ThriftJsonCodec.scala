/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api

import ai.chronon.api.Extensions.StringsOps
import ai.chronon.api.HashUtils.md5Bytes
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.thrift.TBase
import ai.chronon.api.thrift.TDeserializer
import ai.chronon.api.thrift.TSerializer
import ai.chronon.api.thrift.protocol.TCompactProtocol
import ai.chronon.api.thrift.protocol.TSimpleJSONProtocol
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, SerializationFeature}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.util
import java.util.Base64
import scala.io.BufferedSource
import scala.io.Source._
import scala.reflect.ClassTag

object ThriftJsonCodec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient
  private lazy val serializerThreaded: ThreadLocal[TSerializer] = new ThreadLocal[TSerializer] {
    override def initialValue(): TSerializer = new TSerializer(new TSimpleJSONProtocol.Factory())
  }

  private def serializer: TSerializer = serializerThreaded.get()

  def toJsonNode[T <: TBase[_, _]: Manifest](obj: T): JsonNode = {
    val jsonBytes = serializer.serialize(obj)
    val jsonTree = mapper.readTree(jsonBytes)
    sortJsonNode(jsonTree)
  }

  def toJsonStr[T <: TBase[_, _]: Manifest](obj: T): String = {
    mapper.writeValueAsString(toJsonNode(obj))
  }

  private val mapper = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
    mapper
  }

  // Recursively sort all object keys in a JsonNode for canonical representation
  private def sortJsonNode(node: JsonNode): JsonNode = {
    if (node.isObject) {
      val sorted = mapper.createObjectNode()
      val fieldNames = new java.util.TreeSet[String]()
      node.fieldNames().forEachRemaining(fieldNames.add)
      fieldNames.forEach { fieldName =>
        sorted.set(fieldName, sortJsonNode(node.get(fieldName)))
      }
      sorted
    } else if (node.isArray) {
      val sorted = mapper.createArrayNode()
      node.elements().forEachRemaining { element =>
        sorted.add(sortJsonNode(element))
      }
      sorted
    } else {
      node
    }
  }

  def toJsonList[T <: TBase[_, _]: Manifest](obj: util.List[T]): String = {
    if (obj == null) return ""
    obj.toScala
      .map(o => new String(toJsonStr(o)))
      .prettyInline
  }

  /** MetaData is execution and bookkeeping state - name, version, team, tags, schedule - and never
    * describes what a conf computes, so semantic hashes must exclude it wherever it appears. Callers
    * used to unset it field by field, which reached the top-level conf and joinParts' groupBys but
    * not confs embedded through a joinSource: a chained conf inherited its upstream's bookkeeping,
    * and editing a tag upstream churned every downstream hash and node name. Stripping the field
    * recursively from the serialized tree covers every nesting path, including ones added later.
    *
    * ExternalSource spells the field `metadata`, every other struct `metaData`, so the match is
    * case-insensitive.
    */
  private val MetaDataFieldNames = Seq("metaData", "metadata")

  private def isMetaDataField(fieldName: String): Boolean =
    MetaDataFieldNames.exists(_.equalsIgnoreCase(fieldName))

  private[api] def withoutMetaData(node: JsonNode): JsonNode = {
    if (node.isObject) {
      val stripped = mapper.createObjectNode()
      node.fieldNames().forEachRemaining { fieldName =>
        if (!isMetaDataField(fieldName)) stripped.set(fieldName, withoutMetaData(node.get(fieldName)))
      }
      stripped
    } else if (node.isArray) {
      val stripped = mapper.createArrayNode()
      node.elements().forEachRemaining(element => stripped.add(withoutMetaData(element)))
      stripped
    } else {
      node
    }
  }

  /** Guards the invariant, and deliberately does not re-implement [[withoutMetaData]]'s traversal:
    * a guard that repeats the walk it checks shares that walk's blind spots, so a nesting the strip
    * skips would be a nesting the guard skips too. Jackson's findValues does the descent instead -
    * one library call, nothing here to get wrong - and the two agree only if the strip is right.
    */
  private[api] def requireNoMetaData(node: JsonNode): Unit = {
    val leaked = MetaDataFieldNames.filter(fieldName => !node.findValues(fieldName).isEmpty)
    require(
      leaked.isEmpty,
      s"Semantic hash input still carries ${leaked.mkString(" and ")}. " +
        "Semantic hashes must not depend on bookkeeping fields."
    )
  }

  private def semanticJsonStr[T <: TBase[_, _]: Manifest](obj: T): String = {
    val stripped = withoutMetaData(toJsonNode(obj))
    requireNoMetaData(stripped)
    mapper.writeValueAsString(stripped)
  }

  /** [[md5Digest]] over the conf with metaData stripped at every depth. */
  def semanticMd5Digest[T <: TBase[_, _]: Manifest](obj: T): String =
    HashUtils.md5Base64(semanticJsonStr(obj).getBytes(Constants.UTF8))

  def semanticMd5Digest[T <: TBase[_, _]: Manifest](obj: util.List[T]): String = {
    val json = if (obj == null) "" else obj.toScala.map(semanticJsonStr(_)).prettyInline
    HashUtils.md5Base64(json.getBytes(Constants.UTF8))
  }

  /** [[hexDigest]] over the conf with metaData stripped at every depth. */
  def semanticHexDigest[T <: TBase[_, _]: Manifest](obj: T, length: Int = 6): String = {
    val canonicalBytes = semanticJsonStr(obj).getBytes(Constants.UTF8)
    md5Bytes(canonicalBytes).map("%02x".format(_)).mkString.take(length)
  }

  def toCompactBase64[T <: TBase[_, _]: Manifest](obj: T): String = {
    val compactSerializer = new TSerializer(new TCompactProtocol.Factory())
    Base64.getEncoder.encodeToString(compactSerializer.serialize(obj))
  }

  def md5Digest[T <: TBase[_, _]: Manifest](obj: T): String = {
    HashUtils.md5Base64(ThriftJsonCodec.toJsonStr(obj).getBytes(Constants.UTF8))
  }

  def hexDigest[T <: TBase[_, _]: Manifest](obj: T, length: Int = 6): String = {
    val jsonNode = toJsonNode(obj)
    val canonicalBytes = mapper.writeValueAsBytes(jsonNode)
    md5Bytes(canonicalBytes).map("%02x".format(_)).mkString.take(length)
  }

  def md5Digest[T <: TBase[_, _]: Manifest](obj: util.List[T]): String = {
    HashUtils.md5Base64(ThriftJsonCodec.toJsonList(obj).getBytes(Constants.UTF8))
  }

  def fromCompactBase64[T <: TBase[_, _]: Manifest](base: T, base64: String): T = {
    val compactDeserializer = new TDeserializer(new TCompactProtocol.Factory())
    val bytes = Base64.getDecoder.decode(base64)
    try {
      compactDeserializer.deserialize(base, bytes)
      base
    } catch {
      case _: Exception => {
        logger.error("Failed to deserialize using compact protocol, trying Json.")
        fromJsonStr(new String(bytes), check = false, base.getClass)
      }
    }
  }

  def jsonEquals(json1: String, json2: String): Boolean = {
    val node1: JsonNode = mapper.readTree(json1)
    val node2: JsonNode = mapper.readTree(json2)
    sortJsonNode(node1).equals(sortJsonNode(node2))
  }

  def fromJsonStr[T <: TBase[_, _]: Manifest](jsonStr: String, check: Boolean = true, clazz: Class[_ <: T]): T = {
    val obj: T = mapper.readValue(jsonStr, clazz)
    if (check) {
      val reserialized = toJsonStr(obj)
      require(
        jsonEquals(jsonStr, reserialized),
        message = s"""
               |Parsed Json object isn't reversible.
               |Original JSON String:
               |
               |$jsonStr
               |-----------------------------------
               |JSON produced by serializing object:
               |
               |$reserialized
               |------------------------------------
               |""".stripMargin
      )
    }
    obj
  }

  def fromJsonFile[T <: TBase[_, _]: Manifest: ClassTag](fileName: String, check: Boolean): T = {
    fromJsonFile(fromFile(fileName), check)
  }

  def fromJsonFile[T <: TBase[_, _]: Manifest: ClassTag](file: File, check: Boolean): T = {
    fromJsonFile(fromFile(file), check)
  }

  def fromJsonFile[T <: TBase[_, _]: Manifest: ClassTag](src: BufferedSource, check: Boolean): T = {
    val jsonStr =
      try src.mkString
      finally src.close()
    fromJson[T](jsonStr, check)
  }

  def fromJson[T <: TBase[_, _]: Manifest: ClassTag](jsonStr: String, check: Boolean): T = {
    val obj: T = fromJsonStr[T](jsonStr, check, clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
    obj
  }
}
