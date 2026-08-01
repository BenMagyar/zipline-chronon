package ai.chronon.service;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FetcherVerticleTest {

    @Test
    public void parseWarmupJarsTrimsDropsEmptyAndDeduplicatesInOrder() {
        assertEquals(
                List.of("s3://bucket/a.jar", "s3://bucket/b.jar"),
                FetcherVerticle.parseWarmupJars(
                        " s3://bucket/a.jar, ,s3://bucket/b.jar,s3://bucket/a.jar "));
    }

    @Test
    public void parseWarmupJarsTreatsMissingConfigurationAsEmpty() {
        assertEquals(List.of(), FetcherVerticle.parseWarmupJars(null));
        assertEquals(List.of(), FetcherVerticle.parseWarmupJars("  "));
    }

    @Test
    public void jarWarmupContinuesAfterAnIndividualFailure() {
        List<String> attempted = new ArrayList<>();

        boolean completed = FetcherVerticle.prefetchJars(
                List.of("bad.jar", "good.jar"),
                1_000,
                jar -> {
                    attempted.add(jar);
                    if (jar.equals("bad.jar")) {
                        throw new IllegalStateException("missing test jar");
                    }
                    return new File(jar);
                });

        assertTrue(completed);
        assertEquals(List.of("bad.jar", "good.jar"), attempted);
    }

    @Test
    public void jarWarmupCannotHoldStartupPastItsBudget() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);

        boolean completed = FetcherVerticle.prefetchJars(
                List.of("stalled.jar"),
                100,
                jar -> {
                    entered.countDown();
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException error) {
                        interrupted.countDown();
                        Thread.currentThread().interrupt();
                    }
                    return new File(jar);
                });

        assertFalse(completed);
        assertEquals(0, entered.getCount());
        assertTrue(interrupted.await(1, TimeUnit.SECONDS));
    }
}
