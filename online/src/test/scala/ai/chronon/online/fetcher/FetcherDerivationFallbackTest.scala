package ai.chronon.online.fetcher

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class FetcherDerivationFallbackTest extends AnyFlatSpec with Matchers {

  "renameOnlyFallback" should "skip rename fallback for complex derivations" in {
    Fetcher.renameOnlyFallback(
      areDerivationsRenameOnly = false,
      fallback = fail("complex derivations must not evaluate rename-only fallback")
    ) shouldBe Success(Map.empty)
  }

  it should "preserve rename fallback for rename-only derivations" in {
    Fetcher.renameOnlyFallback(
      areDerivationsRenameOnly = true,
      fallback = Map("renamed_value" -> Int.box(1))
    ) shouldBe Success(Map("renamed_value" -> Int.box(1)))
  }
}
