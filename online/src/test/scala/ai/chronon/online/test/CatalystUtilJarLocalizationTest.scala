package ai.chronon.online.test

import ai.chronon.online.CatalystUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}
import java.util.jar.{JarEntry, JarOutputStream}
import scala.collection.JavaConverters._

class CatalystUtilJarLocalizationTest extends AnyFlatSpec with Matchers with CatalystUtilTestSparkSQLStructs {

  private def writeJar(path: Path): Unit = {
    val output = new JarOutputStream(Files.newOutputStream(path))
    try {
      output.putNextEntry(new JarEntry("localization-test.txt"))
      output.write("test".getBytes(StandardCharsets.UTF_8))
      output.closeEntry()
    } finally output.close()
  }

  private def withSourceJar(test: Path => Unit): Unit = {
    val source = Files.createTempFile("catalyst-util-source-", ".jar")
    writeJar(source)
    try test(source)
    finally Files.deleteIfExists(source)
  }

  it should "normalize equivalent S3 jar paths to one cache key" in {
    CatalystUtil.normalizeJarUri("s3://bucket/udfs/example.jar") shouldBe
      CatalystUtil.normalizeJarUri("s3a://bucket/udfs/example.jar")
  }

  it should "reuse one localized jar across distinct blueprints" in withSourceJar { source =>
    val sourceUri = source.toUri.toString
    val jarsBefore = CatalystUtil.localizedJarCount
    try {
      val setups = Seq(s"ADD JAR '$sourceUri'")
      new CatalystUtil(CommonScalarsStruct, Seq("first_value" -> "int32_x"), setups = setups)
      val first = CatalystUtil.localizeJar(sourceUri)

      // The changed select creates a different BlueprintKey and executes its setup independently.
      new CatalystUtil(CommonScalarsStruct, Seq("second_value" -> "int32_x + 1"), setups = setups)
      val second = CatalystUtil.localizeJar(sourceUri)

      first.getCanonicalPath shouldBe second.getCanonicalPath
      first.toPath should not be source
      Files.readAllBytes(first.toPath) shouldBe Files.readAllBytes(source)
      CatalystUtil.localizedJarCount shouldBe jarsBefore + 1
      CatalystUtil.isJarLocalized(sourceUri) shouldBe true
    } finally CatalystUtil.evictLocalizedJarForTest(sourceUri)
  }

  it should "share a single first localization across concurrent callers" in withSourceJar { source =>
    val sourceUri = source.toUri.toString
    val jarsBefore = CatalystUtil.localizedJarCount
    val threads = 24
    val executor = Executors.newFixedThreadPool(threads)
    val ready = new CountDownLatch(threads)
    val go = new CountDownLatch(1)
    val done = new CountDownLatch(threads)
    val localizedPaths = new ConcurrentLinkedQueue[String]()
    val failures = new ConcurrentLinkedQueue[Throwable]()

    try {
      (0 until threads).foreach { _ =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            ready.countDown()
            try {
              go.await()
              localizedPaths.add(CatalystUtil.localizeJar(sourceUri).getCanonicalPath)
            } catch {
              case throwable: Throwable => failures.add(throwable)
            } finally done.countDown()
          }
        })
      }

      ready.await(30, TimeUnit.SECONDS) shouldBe true
      go.countDown()
      done.await(60, TimeUnit.SECONDS) shouldBe true

      withClue(failures.asScala.mkString("; ")) {
        failures shouldBe empty
      }
      localizedPaths.asScala.toSet should have size 1
      localizedPaths should have size threads.toLong
      CatalystUtil.localizedJarCount shouldBe jarsBefore + 1
    } finally {
      go.countDown()
      executor.shutdownNow()
      CatalystUtil.evictLocalizedJarForTest(sourceUri)
    }
  }

  it should "give different source URIs with the same basename distinct local jar names" in {
    val firstDirectory = Files.createTempDirectory("catalyst-util-first-")
    val secondDirectory = Files.createTempDirectory("catalyst-util-second-")
    val firstSource = firstDirectory.resolve("out.jar")
    val secondSource = secondDirectory.resolve("out.jar")
    writeJar(firstSource)
    writeJar(secondSource)
    val firstUri = firstSource.toUri.toString
    val secondUri = secondSource.toUri.toString

    try {
      val first = CatalystUtil.localizeJar(firstUri)
      val second = CatalystUtil.localizeJar(secondUri)

      first.getName should not be second.getName
      first.getName should endWith(".jar")
      second.getName should endWith(".jar")
    } finally {
      CatalystUtil.evictLocalizedJarForTest(firstUri)
      CatalystUtil.evictLocalizedJarForTest(secondUri)
      Files.deleteIfExists(firstSource)
      Files.deleteIfExists(secondSource)
      Files.deleteIfExists(firstDirectory)
      Files.deleteIfExists(secondDirectory)
    }
  }

  it should "retry localization after a failed attempt" in {
    val directory = Files.createTempDirectory("catalyst-util-retry-")
    val source = directory.resolve("eventual.jar")
    val sourceUri = source.toUri.toString
    val jarsBefore = CatalystUtil.localizedJarCount

    try {
      an[Exception] should be thrownBy CatalystUtil.localizeJar(sourceUri)
      CatalystUtil.localizedJarCount shouldBe jarsBefore
      CatalystUtil.isJarLocalized(sourceUri) shouldBe false

      writeJar(source)
      val localized = CatalystUtil.localizeJar(sourceUri)
      localized.exists() shouldBe true
      Files.readAllBytes(localized.toPath) shouldBe Files.readAllBytes(source)
    } finally {
      CatalystUtil.evictLocalizedJarForTest(sourceUri)
      Files.deleteIfExists(source)
      Files.deleteIfExists(directory)
    }
  }
}
