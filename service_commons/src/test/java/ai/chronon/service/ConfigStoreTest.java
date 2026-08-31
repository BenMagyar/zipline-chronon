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
        System.clearProperty("db.password");
        System.clearProperty("auth_secret");
        System.clearProperty("AUTH_SECRET");
        System.clearProperty("PGPASSWORD");
        System.clearProperty("gcp.credential");
        System.clearProperty("online.jar");
        System.clearProperty("online.api.props");
        System.clearProperty("password_hint");
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

    // /config used to leak plaintext credentials for any key it didn't explicitly know about
    // (e.g. db.password, auth_secret). Any key ending in password/secret/credential now has its
    // value masked, with the last 4 chars preserved when length > 4 for operator correlation.
    @Test
    void encodeConfigMasksTopLevelSecretsWithLastFourTail() {
        System.setProperty("db.password", "supersecretvalue");
        System.setProperty("auth_secret", "abcdefghij");

        ConfigStore cfgStore = new ConfigStore(vertx);
        String encoded = cfgStore.encodeConfig();

        assertFalse(encoded.contains("supersecretvalue"));
        assertFalse(encoded.contains("abcdefghij"));
        assertTrue(encoded.contains("******alue"));
        assertTrue(encoded.contains("******ghij"));
    }

    // Values short enough that revealing 4 trailing chars would leak most of the secret must
    // fall back to the tail-less mask.
    @Test
    void encodeConfigMasksShortSecretsWithoutTail() {
        System.setProperty("db.password", "abcd");

        ConfigStore cfgStore = new ConfigStore(vertx);
        String encoded = cfgStore.encodeConfig();

        assertFalse(encoded.contains("abcd"));
        assertTrue(encoded.contains("\"db.password\" : \"******\""));
    }

    // Suffix match must be case-insensitive and cover all three suffixes.
    @Test
    void encodeConfigMasksIsCaseInsensitiveAcrossSuffixes() {
        System.setProperty("AUTH_SECRET", "longenoughvalue");
        System.setProperty("PGPASSWORD", "anotherlongvalue");
        System.setProperty("gcp.credential", "yetanotherlongvalue");

        ConfigStore cfgStore = new ConfigStore(vertx);
        String encoded = cfgStore.encodeConfig();

        assertFalse(encoded.contains("longenoughvalue"));
        assertFalse(encoded.contains("anotherlongvalue"));
        assertFalse(encoded.contains("yetanotherlongvalue"));
        assertTrue(encoded.contains("******alue"));
    }

    // Nested JsonObject case — online.api.props is the primary vehicle for third-party creds
    // (see example_config.json). Vert.x's sys-props store parses JSON-shaped values into a
    // JsonObject, exercising the recursive redaction path.
    @Test
    void encodeConfigMasksSecretsNestedInsideJsonObject() {
        System.setProperty("online.api.props", "{\"user\":\"admin\",\"password\":\"nestedpassvalue\",\"host\":\"localhost\"}");

        ConfigStore cfgStore = new ConfigStore(vertx);
        String encoded = cfgStore.encodeConfig();

        assertFalse(encoded.contains("nestedpassvalue"));
        assertTrue(encoded.contains("******alue"));
        assertTrue(encoded.contains("\"user\" : \"admin\""));
        assertTrue(encoded.contains("\"host\" : \"localhost\""));
    }

    // Non-secret keys must not be touched by the redaction pass.
    @Test
    void encodeConfigLeavesNonSecretKeysUntouched() {
        System.setProperty("online.jar", "/path/to/api.jar");
        System.setProperty("db.password", "supersecretvalue");

        ConfigStore cfgStore = new ConfigStore(vertx);
        String encoded = cfgStore.encodeConfig();

        assertTrue(encoded.contains("/path/to/api.jar"));
        assertFalse(encoded.contains("supersecretvalue"));
    }

    // A secret-named key whose value happens to be a nested object/array must have the entire
    // subtree collapsed — we can't derive a meaningful correlation tail from a composite and
    // recursing risks leaking subfields that only appear innocuous.
    @Test
    void encodeConfigMasksEntireSubtreeUnderSecretNamedParent() {
        System.setProperty("db.password", "{\"value\":\"actualsecretvalue\",\"set_at\":\"2026-01-01\"}");

        ConfigStore cfgStore = new ConfigStore(vertx);
        String encoded = cfgStore.encodeConfig();

        assertFalse(encoded.contains("actualsecretvalue"));
        assertFalse(encoded.contains("set_at"));
        assertFalse(encoded.contains("2026-01-01"));
        assertTrue(encoded.contains("\"db.password\" : \"******\""));
    }

    // Extension of encodeConfigDoesNotMutateLiveConfig: redaction of nested credentials must
    // not corrupt the live config either. Locks in the deep-copy in encodeConfig().
    @Test
    void encodeConfigDoesNotMutateNestedLiveConfig() {
        System.setProperty("online.api.props", "{\"user\":\"admin\",\"password\":\"nestedpassvalue\"}");

        ConfigStore cfgStore = new ConfigStore(vertx);
        cfgStore.encodeConfig();

        assertEquals("nestedpassvalue", cfgStore.getOnlineApiProps().get("password"));
    }

    // Mid-key occurrences of secret-like words must NOT be masked — the suffix rule is
    // intentionally narrower than a substring match so keys like `password_hint` pass through.
    @Test
    void encodeConfigDoesNotMaskMidKeyMatches() {
        System.setProperty("password_hint", "not-actually-a-secret");

        ConfigStore cfgStore = new ConfigStore(vertx);
        String encoded = cfgStore.encodeConfig();

        assertTrue(encoded.contains("not-actually-a-secret"));
    }
}
