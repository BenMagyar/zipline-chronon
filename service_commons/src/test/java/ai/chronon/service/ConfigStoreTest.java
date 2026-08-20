package ai.chronon.service;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigStoreTest {

    private Vertx vertx;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown() {
        vertx.close();
        System.clearProperty("ai.chronon.warmup.join.regex");
        System.clearProperty("ai.chronon.warmup.payload");
        System.clearProperty("ai.chronon.warmup.join.0.regex");
        System.clearProperty("ai.chronon.warmup.join.0.payload");
        System.clearProperty("ai.chronon.warmup.join.1.regex");
        System.clearProperty("ai.chronon.warmup.join.1.payload");
    }

    // Locks in that upgrading to a version with the compile-touch / periodic-rewarmup feature doesn't change
    // any existing deployment's behavior unless it explicitly opts in via WARMUP_COMPILE_ALL_ONLINE_JOINS /
    // WARMUP_PERIODIC_ENABLED.
    @Test
    void warmupCompileAndPeriodicDefaultToDisabled() {
        ConfigStore cfgStore = new ConfigStore(vertx);

        assertFalse(cfgStore.isWarmupCompileAllOnlineJoinsEnabled());
        assertFalse(cfgStore.isWarmupPeriodicEnabled());
        assertEquals(0L, cfgStore.getWarmupCompileWaitTimeoutMillis());
    }

    // JsonObject(Map) doesn't copy its backing map - encodeConfig() used to remove() straight from that
    // shared map, permanently deleting WARMUP_PAYLOAD from the live config on the first call.
    @Test
    void encodeConfigDoesNotMutateLiveConfig() {
        System.setProperty("ai.chronon.warmup.join.regex", ".*");
        System.setProperty("ai.chronon.warmup.payload", "payload0");

        ConfigStore cfgStore = new ConfigStore(vertx);
        cfgStore.encodeConfig();

        assertEquals(1, cfgStore.getWarmupGroups().size());
    }

    // encodeConfig() used to redact only the single-group WARMUP_PAYLOAD key, leaking indexed
    // multi-group payloads (ai.chronon.warmup.join.%d.payload) via /config.
    @Test
    void encodeConfigRedactsSingleAndIndexedGroupPayloads() {
        System.setProperty("ai.chronon.warmup.payload", "single-group-secret");
        System.setProperty("ai.chronon.warmup.join.0.regex", ".*a.*");
        System.setProperty("ai.chronon.warmup.join.0.payload", "indexed-group-secret");

        ConfigStore cfgStore = new ConfigStore(vertx);
        String encoded = cfgStore.encodeConfig();

        assertFalse(encoded.contains("single-group-secret"));
        assertFalse(encoded.contains("indexed-group-secret"));
    }

    // getWarmupGroups() used to break the whole scan at the first missing payload, silently discarding
    // every valid group configured at a later index.
    @Test
    void getWarmupGroupsSkipsMissingPayloadWithoutDroppingLaterGroups() {
        System.setProperty("ai.chronon.warmup.join.0.regex", ".*a.*");
        // index 0 payload deliberately left unset
        System.setProperty("ai.chronon.warmup.join.1.regex", ".*b.*");
        System.setProperty("ai.chronon.warmup.join.1.payload", "payload1");

        ConfigStore cfgStore = new ConfigStore(vertx);
        List<ConfigStore.WarmupGroup> groups = cfgStore.getWarmupGroups();

        assertEquals(1, groups.size());
        assertEquals(".*b.*", groups.get(0).regex);
        assertEquals("payload1", groups.get(0).payload);
    }
}
