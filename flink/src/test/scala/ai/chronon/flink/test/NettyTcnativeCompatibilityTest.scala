package ai.chronon.flink.test

import io.netty.util.Version
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NettyTcnativeCompatibilityTest extends AnyFlatSpec with Matchers {

  "The Netty native SSL classes" should "match the Netty runtime API" in {
    val nettyVersion = Version.identify().get("netty-common").artifactVersion()

    if (nettyVersion.startsWith("4.2.")) {
      val sslClass = Class.forName("io.netty.internal.tcnative.SSL", false, getClass.getClassLoader)
      noException should be thrownBy sslClass.getDeclaredMethod("getGroupName", java.lang.Long.TYPE)
    }
  }
}
