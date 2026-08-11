package ai.chronon.service;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigStoreTest {

    @Test
    void readsIndexedWarmupGroupsAndDefaultsToOneRequest() {
        JsonObject userPayload = new JsonArray()
                .add(new JsonObject().put("user_id", "1"))
                .getJsonObject(0);
        JsonObject config = new JsonObject()
                .put("ai.chronon.warmup.join.0.regex", "^ranking\\.search\\.attempt__51$")
                .put("ai.chronon.warmup.join.0.payload", new JsonArray().add(userPayload))
                .put("ai.chronon.warmup.join.1.regex", "^ranking\\.user\\.views\\.enriched__11$")
                .put("ai.chronon.warmup.join.1.payload", "[{\"product_id\":5}]");

        ConfigStore store = new ConfigStore(config);
        List<ConfigStore.WarmupGroup> groups = store.getWarmupGroups();

        assertEquals(2, groups.size());
        assertEquals("^ranking\\.search\\.attempt__51$", groups.get(0).regex);
        assertEquals("[{\"user_id\":\"1\"}]", groups.get(0).payload);
        assertEquals("[{\"product_id\":5}]", groups.get(1).payload);
        assertEquals(1, store.getWarmupTimesPerJoin());
    }

    @Test
    void redactsSingleAndIndexedWarmupPayloadsFromConfigEndpoint() {
        JsonObject config = new JsonObject()
                .put("server.port", 8080)
                .put("ai.chronon.warmup.join.regex", "single")
                .put("ai.chronon.warmup.payload", "single-secret")
                .put("ai.chronon.warmup.join.0.regex", "first")
                .put("ai.chronon.warmup.join.0.payload", "first-secret")
                .put("ai.chronon.warmup.join.7.payload", "unread-but-still-secret");

        JsonObject encoded = new JsonObject(new ConfigStore(config).encodeConfig());

        assertEquals(8080, encoded.getInteger("server.port"));
        assertTrue(encoded.containsKey("ai.chronon.warmup.join.regex"));
        assertTrue(encoded.containsKey("ai.chronon.warmup.join.0.regex"));
        assertFalse(encoded.containsKey("ai.chronon.warmup.payload"));
        assertFalse(encoded.containsKey("ai.chronon.warmup.join.0.payload"));
        assertFalse(encoded.containsKey("ai.chronon.warmup.join.7.payload"));
    }
}
