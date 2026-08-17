package ai.chronon.api.test

import ai.chronon.api._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Collections
import scala.jdk.CollectionConverters._

/** The semantic digests exclude metaData at every depth, so a conf's hash tracks what it computes
  * and not the bookkeeping hanging off it or off anything it embeds.
  */
class SemanticDigestTest extends AnyFlatSpec with Matchers {

  private def eventSource(table: String): Source = {
    val query = new Query()
    query.setSelects(Collections.singletonMap("user_id", "user_id"))
    val source = new Source()
    source.setEvents(new EventSource().setTable(table).setQuery(query))
    source
  }

  private def groupBy(name: String, table: String = "ns.events", tags: Map[String, String] = Map.empty): GroupBy = {
    val metaData = new MetaData().setName(name).setTeam("some_team").setVersion("0")
    if (tags.nonEmpty) metaData.setTags(tags.asJava)
    new GroupBy()
      .setMetaData(metaData)
      .setSources(Collections.singletonList(eventSource(table)))
      .setKeyColumns(Collections.singletonList("user_id"))
  }

  private def join(name: String, part: GroupBy, leftTable: String = "ns.left"): Join =
    new Join()
      .setMetaData(new MetaData().setName(name).setTeam("some_team"))
      .setLeft(eventSource(leftTable))
      .setJoinParts(Collections.singletonList(new JoinPart().setGroupBy(part)))

  private def chainedGroupBy(upstream: Join, name: String): GroupBy = {
    val joinSource = new JoinSource().setJoin(upstream).setQuery(new Query())
    val source = new Source()
    source.setJoinSource(joinSource)
    new GroupBy()
      .setMetaData(new MetaData().setName(name))
      .setSources(Collections.singletonList(source))
      .setKeyColumns(Collections.singletonList("user_id"))
  }

  "semanticMd5Digest" should "ignore metaData on the conf itself" in {
    val plain = groupBy("team.gb")
    val edited = groupBy("team.renamed_gb", tags = Map("owner" -> "team-a"))

    ThriftJsonCodec.semanticMd5Digest(edited) should equal(ThriftJsonCodec.semanticMd5Digest(plain))
    // the unstripped digest is what a cache key wants, and it must still see the difference
    ThriftJsonCodec.md5Digest(edited) should not equal ThriftJsonCodec.md5Digest(plain)
  }

  it should "ignore metaData nested arbitrarily deep" in {
    val plain = chainedGroupBy(join("team.upstream", groupBy("team.gb")), "team.chained")
    val edited = chainedGroupBy(
      join("team.upstream_renamed", groupBy("team.gb", tags = Map("tier" -> "gold"))),
      "team.chained"
    )

    ThriftJsonCodec.semanticMd5Digest(edited) should equal(ThriftJsonCodec.semanticMd5Digest(plain))
  }

  it should "still distinguish confs whose content differs" in {
    val plain = chainedGroupBy(join("team.upstream", groupBy("team.gb", table = "ns.events")), "team.chained")
    val edited = chainedGroupBy(join("team.upstream", groupBy("team.gb", table = "ns.other_events")), "team.chained")

    ThriftJsonCodec.semanticMd5Digest(edited) should not equal ThriftJsonCodec.semanticMd5Digest(plain)
  }

  it should "match the unstripped digest when the conf carries no metaData at all" in {
    // existing confs whose hashable copy was already metaData-free must not churn
    val bare = groupBy("team.gb")
    bare.unsetMetaData()

    ThriftJsonCodec.semanticMd5Digest(bare) should equal(ThriftJsonCodec.md5Digest(bare))
  }

  it should "ignore ExternalSource's lower-case `metadata` spelling" in {
    def externalPart(name: String): ExternalPart = {
      val schema = new TDataType().setKind(DataKind.STRUCT)
      val source = new ExternalSource()
        .setMetadata(new MetaData().setName(name))
        .setKeySchema(schema)
        .setValueSchema(schema)
      new ExternalPart().setSource(source)
    }

    ThriftJsonCodec.semanticMd5Digest(externalPart("team.ext_b")) should equal(
      ThriftJsonCodec.semanticMd5Digest(externalPart("team.ext_a")))
  }

  "semanticHexDigest" should "ignore metaData nested arbitrarily deep" in {
    val plain = chainedGroupBy(join("team.upstream", groupBy("team.gb")), "team.chained")
    val edited = chainedGroupBy(join("team.upstream_renamed", groupBy("team.gb")), "team.chained")

    ThriftJsonCodec.semanticHexDigest(edited) should equal(ThriftJsonCodec.semanticHexDigest(plain))
    ThriftJsonCodec.hexDigest(edited) should not equal ThriftJsonCodec.hexDigest(plain)
  }

  "the metaData guard" should "reject a hash input that still carries metaData" in {
    // stands in for a future strip that stops recursing: the digest must fail loudly rather than
    // quietly couple semantic hashes to bookkeeping again
    val withMetaData = ThriftJsonCodec.toJsonNode(groupBy("team.gb"))

    val thrown = the[IllegalArgumentException] thrownBy ThriftJsonCodec.requireNoMetaData(withMetaData)
    thrown.getMessage should include("metaData")

    noException should be thrownBy ThriftJsonCodec.requireNoMetaData(ThriftJsonCodec.withoutMetaData(withMetaData))
  }

  it should "catch metaData nested under objects and arrays" in {
    // hand-built rather than routed through withoutMetaData: the guard has to hold on trees the
    // strip never saw, which is the whole reason it does not reuse the strip's traversal
    val mapper = new ObjectMapper()

    def leaf(fieldName: String): ObjectNode = {
      val node = mapper.createObjectNode()
      node.putObject(fieldName).put("name", "team.leaked")
      node
    }

    val underArray = mapper.createObjectNode()
    underArray.putArray("joinParts").add(leaf("metaData"))

    val deeplyNested = mapper.createObjectNode()
    deeplyNested.putObject("sources").replace("joinSource", leaf("metaData"))

    val externalSpelling = leaf("metadata")

    Seq(underArray, deeplyNested, externalSpelling).foreach { node =>
      an[IllegalArgumentException] should be thrownBy ThriftJsonCodec.requireNoMetaData(node)
    }

    noException should be thrownBy ThriftJsonCodec.requireNoMetaData(
      mapper.createObjectNode().put("keyColumns", "user_id"))
  }
}
