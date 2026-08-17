package ai.chronon.api.test.planner

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.{JoinPlanner, MonolithJoinPlanner}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** metaData is execution/status bookkeeping, never semantics: the planners already drop it from the
  * conf they hash. They dropped it only from the top level and from joinParts' groupBys, so a
  * chained conf -- whose upstream lives inside a joinSource -- carried its upstream's metaData into
  * every downstream hash, and unrelated bookkeeping edits churned the hash.
  */
class JoinSourceMetaDataHashTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def upstreamJoin(): Join =
    Builders.Join(
      metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_join"),
      left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.upstream_left"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = Builders.GroupBy(
          sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.upstream_events")),
          keyColumns = Seq("user_id"),
          aggregations = Seq(Builders.Aggregation(Operation.COUNT, "event_count", Seq(WindowUtils.Unbounded))),
          metaData = Builders.MetaData(namespace = "test_namespace", name = "upstream_gb"),
          accuracy = Accuracy.SNAPSHOT
        )))
    )

  private def chainedJoin(upstream: Join): Join =
    Builders.Join(
      metaData = Builders.MetaData(namespace = "test_namespace", name = "downstream_join"),
      left = Builders.Source.joinSource(
        upstream,
        Builders.Query(selects = Builders.Selects("user_id", "ts"), partitionColumn = "ds")
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = Builders.GroupBy(
          sources = Seq(Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.listing_events")),
          keyColumns = Seq("user_id"),
          aggregations = Seq(Builders.Aggregation(Operation.LAST, "price", Seq(WindowUtils.Unbounded))),
          metaData = Builders.MetaData(namespace = "test_namespace", name = "downstream_gb"),
          accuracy = Accuracy.TEMPORAL
        )))
    )

  private def chainedGroupBy(upstream: Join): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.joinSource(
          upstream,
          Builders.Query(selects = Builders.Selects("user_id", "ts"), partitionColumn = "ds")
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "chained_count", Seq(WindowUtils.Unbounded))),
      metaData = Builders.MetaData(namespace = "test_namespace", name = "chained_gb"),
      accuracy = Accuracy.SNAPSHOT
    )

  private def nodeFingerprints(join: Join): Seq[(String, String)] = {
    val modular = new JoinPlanner(join).buildPlan.nodes.asScala
    val monolith = MonolithJoinPlanner(join).buildPlan.nodes.asScala
    (modular ++ monolith).map(n => n.metaData.name -> n.semanticHash).toSeq
  }

  "Join node semantic hashes" should "ignore bookkeeping metaData on a join reached through a joinSource" in {
    val plain = chainedJoin(upstreamJoin())

    val edited = chainedJoin({
      val up = upstreamJoin()
      up.metaData.setTags(Map("owner" -> "team-a").asJava)
      up.metaData.setCustomJson("""{"note": "edited"}""")
      up.metaData.setTeam("some_other_team")
      up
    })

    nodeFingerprints(edited) should equal(nodeFingerprints(plain))
  }

  it should "ignore bookkeeping metaData on a groupBy nested inside a joinSource's join" in {
    val plain = chainedJoin(upstreamJoin())

    val edited = chainedJoin({
      val up = upstreamJoin()
      up.joinParts.get(0).groupBy.metaData.setTags(Map("tier" -> "gold").asJava)
      up
    })

    nodeFingerprints(edited) should equal(nodeFingerprints(plain))
  }

  it should "still change when the upstream join's actual content changes" in {
    val plain = chainedJoin(upstreamJoin())

    val edited = chainedJoin({
      val up = upstreamJoin()
      up.joinParts.get(0).groupBy.setKeyColumns(Seq("listing_id").asJava)
      up
    })

    nodeFingerprints(edited) should not equal nodeFingerprints(plain)
  }

  /** The chaining need not be on the left: a join whose left is a plain events source still reaches
    * a joinSource through a joinPart's groupBy. Unsetting metaData on each joinPart's groupBy stops
    * one level short of that upstream, so this shape leaks where the left-chained one is covered.
    * Modelled on canary's azure.scd2_under18_contacts.claim_spine_join__1.
    */
  it should "ignore bookkeeping metaData reached through a joinPart's chained groupBy" in {
    def joinOverChainedPart(upstream: Join): Join =
      Builders.Join(
        metaData = Builders.MetaData(namespace = "test_namespace", name = "spine_join"),
        left = Builders.Source.events(Builders.Query(partitionColumn = "ds"), table = "test.spine"),
        joinParts = Seq(Builders.JoinPart(groupBy = chainedGroupBy(upstream)))
      )

    val plain = joinOverChainedPart(upstreamJoin())

    val edited = joinOverChainedPart({
      val up = upstreamJoin()
      up.metaData.setTags(Map("owner" -> "team-a").asJava)
      up.joinParts.get(0).groupBy.metaData.setCustomJson("""{"note": "edited"}""")
      up
    })

    nodeFingerprints(edited) should equal(nodeFingerprints(plain))
  }

  /** Node names carry the hash, so a leaked hash renamed physical tables and not just the hash
    * column: the left source table is `<left>__<leftSourceHash>__source`.
    */
  it should "keep the left source table name stable when only upstream metaData changes" in {
    def leftSourceNodeName(join: Join): String =
      new JoinPlanner(join).buildPlan.nodes.asScala.find(_.content.isSetSourceWithFilter).get.metaData.name

    val plain = chainedJoin(upstreamJoin())

    val edited = chainedJoin({
      val up = upstreamJoin()
      up.metaData.setTags(Map("owner" -> "team-a").asJava)
      up
    })

    leftSourceNodeName(edited) should equal(leftSourceNodeName(plain))
  }

  "GroupBy.semanticHash" should "ignore bookkeeping metaData on a join reached through a joinSource" in {
    val plain = chainedGroupBy(upstreamJoin())

    val edited = chainedGroupBy({
      val up = upstreamJoin()
      up.metaData.setTags(Map("owner" -> "team-a").asJava)
      up.metaData.setCustomJson("""{"note": "edited"}""")
      up
    })

    edited.semanticHash should equal(plain.semanticHash)
  }

  it should "still change when the upstream join's actual content changes" in {
    val plain = chainedGroupBy(upstreamJoin())

    val edited = chainedGroupBy({
      val up = upstreamJoin()
      up.joinParts.get(0).groupBy.setKeyColumns(Seq("listing_id").asJava)
      up
    })

    edited.semanticHash should not equal plain.semanticHash
  }
}
