package ai.chronon.service;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import ai.chronon.online.metrics.TTLCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Helps keep track of the various Chronon service configs.
 * We currently read configs once at startup - this makes sense for configs
 * such as the server port and we can revisit / extend things in the future to
 * be able to hot-refresh configs like Vertx supports under the hood.
 */
public class ConfigStore {

    private static final Logger logger = LoggerFactory.getLogger(ConfigStore.class);

    private static final int DEFAULT_PORT = 8080;

    private static final String SERVER_PORT = "server.port";
    private static final String ONLINE_JAR = "online.jar";
    private static final String ONLINE_CLASS = "online.class";
    private static final String ONLINE_API_PROPS = "online.api.props";
    
    // Database configuration
    private static final String JDBC_URL = "db.url";
    private static final String JDBC_USERNAME = "db.username";
    private static final String JDBC_PASSWORD = "db.password";
    
    // GCP configuration
    private static final String GCP_PROJECT_ID = "gcp.projectId";

    // TTL cache configuration
    private static final String JOIN_CONF_TTL_MILLIS = "ai.chronon.join.conf.ttl.millis";
    private static final String JOIN_CODEC_TTL_MILLIS = "ai.chronon.join.codec.ttl.millis";

    // Warmup configuration
    // Single-group convenience keys (equivalent to index 0)
    private static final String WARMUP_JOIN_REGEX = "ai.chronon.warmup.join.regex";
    private static final String WARMUP_PAYLOAD = "ai.chronon.warmup.payload";
    private static final String WARMUP_TIMES_PER_JOIN = "ai.chronon.warmup.times.per.join";
    // Multi-group indexed keys: ai.chronon.warmup.join.N.regex / ai.chronon.warmup.join.N.payload
    private static final String WARMUP_JOIN_INDEXED_REGEX_FMT = "ai.chronon.warmup.join.%d.regex";
    private static final String WARMUP_JOIN_INDEXED_PAYLOAD_FMT = "ai.chronon.warmup.join.%d.payload";
    // Compile-touch (FastSerde) + periodic re-check configuration
    private static final String WARMUP_COMPILE_ALL_ONLINE_JOINS = "ai.chronon.warmup.compile.all.online.joins";
    private static final String WARMUP_PERIODIC_ENABLED = "ai.chronon.warmup.periodic.enabled";
    private static final String WARMUP_PERIODIC_INTERVAL_SECONDS = "ai.chronon.warmup.periodic.interval.seconds";
    private static final long DEFAULT_WARMUP_PERIODIC_INTERVAL_SECONDS = 300L;
    private static final long MIN_WARMUP_PERIODIC_INTERVAL_SECONDS = 30L;
    // Opt-in: unset/0 (default) never blocks startup waiting for FastSerde compiles to finish - they continue
    // in the background as today. A positive value bounds how long warmup blocks waiting for them.
    private static final String WARMUP_COMPILE_WAIT_TIMEOUT_SECONDS = "ai.chronon.warmup.compile.wait.timeout.seconds";
    private static final long MAX_WARMUP_COMPILE_WAIT_TIMEOUT_SECONDS = 120L;

    /** A single warmup group: a regex to match join names and the JSON payload to send. */
    public static class WarmupGroup {
        public final String regex;
        public final String payload;

        public WarmupGroup(String regex, String payload) {
            this.regex = regex;
            this.payload = payload;
        }
    }

    private volatile JsonObject jsonConfig;
    private final Object lock = new Object();

    public ConfigStore(Vertx vertx) {
        // Use CountDownLatch to wait for config loading
        CountDownLatch latch = new CountDownLatch(1);
        ConfigRetriever configRetriever = ConfigRetriever.create(vertx);
        configRetriever.getConfig().onComplete(ar -> {
            if (ar.failed()) {
                throw new IllegalStateException("Unable to load service config", ar.cause());
            }
            synchronized (lock) {
                jsonConfig = ar.result();
            }
            latch.countDown();
        });
        try {
            if (!latch.await(1, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for Vertx config read");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while loading config", e);
        }
    }

    public int getServerPort() {
        return jsonConfig.getInteger(SERVER_PORT, DEFAULT_PORT);
    }

    public Optional<String> getOnlineJar() {
        return Optional.ofNullable(jsonConfig.getString(ONLINE_JAR));
    }

    public Optional<String> getOnlineClass() {
        return Optional.ofNullable(jsonConfig.getString(ONLINE_CLASS));
    }

    public void validateOnlineApiConfig() {
        if (!(getOnlineJar().isPresent() && getOnlineClass().isPresent())) {
            throw new IllegalArgumentException("Both 'online.jar' and 'online.class' configs must be set.");
        }
    }

    public Map<String, String> getOnlineApiProps() {
        JsonObject apiProps = jsonConfig.getJsonObject(ONLINE_API_PROPS);
        if (apiProps == null) {
            return new HashMap<String, String>();
        }

        return apiProps.stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> String.valueOf(e.getValue())
        ));
    }

    /**
     * Gets the JDBC URL for database connection.
     * 
     * @return the JDBC URL
     */
    public String getJdbcUrl() {
        return jsonConfig.getString(JDBC_URL);
    }
    
    /**
     * Gets the JDBC username for database connection.
     * 
     * @return the JDBC username
     */
    public String getJdbcUsername() {
        return jsonConfig.getString(JDBC_USERNAME);
    }
    
    /**
     * Gets the JDBC password for database connection.
     * 
     * @return the JDBC password
     */
    public String getJdbcPassword() {
        return jsonConfig.getString(JDBC_PASSWORD);
    }
    
    /**
     * Gets the GCP project ID.
     * 
     * @return the GCP project ID
     */
    public String getGcpProjectId() {
        return jsonConfig.getString(GCP_PROJECT_ID);
    }
    
    /**
     * Validates database configuration.
     * Ensures all required database properties are set.
     * 
     * @throws IllegalArgumentException if any required property is missing
     */
    public void validateDatabaseConfig() {
        if (getJdbcUrl() == null || getJdbcUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("Database URL is required. Please set 'db.url'.");
        }
        if (getJdbcUsername() == null || getJdbcUsername().trim().isEmpty()) {
            throw new IllegalArgumentException("Database username is required. Please set 'db.username'.");
        }
        if (getJdbcPassword() == null || getJdbcPassword().trim().isEmpty()) {
            throw new IllegalArgumentException("Database password is required. Please set 'db.password'.");
        }
    }
    
    /**
     * Validates GCP configuration.
     * Ensures all required GCP properties are set.
     * 
     * @throws IllegalArgumentException if any required property is missing
     */
    public void validateGcpConfig() {
        if (getGcpProjectId() == null || getGcpProjectId().trim().isEmpty()) {
            throw new IllegalArgumentException("GCP project ID is required. Please set 'gcp.projectId'.");
        }
    }
    
    /**
     * Validates all required configuration.
     * This includes database and GCP configurations.
     * 
     * @throws IllegalArgumentException if any required configuration is invalid
     */
    public void validateAllConfig() {
        validateDatabaseConfig();
        validateGcpConfig();
    }

    /**
     * Returns all configured warmup groups in order.
     *
     * Supports two configuration styles that are merged in order:
     *   1. Single-group convenience: WARMUP_JOIN_REGEX + WARMUP_PAYLOAD (equivalent to index 0)
     *   2. Indexed multi-group: WARMUP_JOIN_N_REGEX + WARMUP_JOIN_N_PAYLOAD for N = 0, 1, 2, ...
     *      Scanning stops at the first missing index.
     *
     * Groups with a missing or unparseable payload are skipped with a warning.
     */
    public List<WarmupGroup> getWarmupGroups() {
        List<WarmupGroup> groups = new ArrayList<>();

        // Single-group convenience keys
        String singleRegex = jsonConfig.getString(WARMUP_JOIN_REGEX);
        String singlePayload = getRawString(WARMUP_PAYLOAD);
        if (singleRegex != null && singlePayload != null) {
            groups.add(new WarmupGroup(singleRegex, singlePayload));
        }

        // Indexed multi-group keys — stop at first missing index
        for (int i = 0; ; i++) {
            String regexKey = String.format(WARMUP_JOIN_INDEXED_REGEX_FMT, i);
            String payloadKey = String.format(WARMUP_JOIN_INDEXED_PAYLOAD_FMT, i);
            String regex = jsonConfig.getString(regexKey);
            if (regex == null) break;
            String payload = getRawString(payloadKey);
            if (payload == null) {
                logger.warn("Warmup group at index {} has regex '{}' but no payload ('{}' missing), skipping", i, regex, payloadKey);
                continue;
            }
            groups.add(new WarmupGroup(regex, payload));
        }

        return groups;
    }

    // Vert.x parses JSON-shaped system property values into JsonArray/JsonObject,
    // so getString() returns null for array-valued payloads. getValue() + toString()
    // recovers the original JSON string regardless of what type Vert.x coerced it to.
    private String getRawString(String key) {
        Object value = jsonConfig.getValue(key);
        if (value == null) return null;
        return value instanceof String ? (String) value : value.toString();
    }

    public int getWarmupTimesPerJoin() {
        int value = jsonConfig.getInteger(WARMUP_TIMES_PER_JOIN, 100);
        if (value <= 0) {
            throw new IllegalArgumentException("WARMUP_TIMES_PER_JOIN must be a positive integer, got: " + value);
        }
        return Math.min(value, 1000);
    }

    // Whether to best-effort trigger FastSerde class generation for every online join's (and its GroupBys')
    // schemas at startup, ahead of live traffic — no payload required. Opt-in (defaults off) so upgrading to a
    // version with this feature doesn't change any existing deployment's behavior without an explicit choice.
    // Separate from the regex/payload-configured groups above, which additionally replay real traffic against
    // the KV store.
    public boolean isWarmupCompileAllOnlineJoinsEnabled() {
        return jsonConfig.getBoolean(WARMUP_COMPILE_ALL_ONLINE_JOINS, false);
    }

    // Whether to periodically re-list online joins and warm up any that came online after startup. Opt-in
    // (defaults off) for the same reason as isWarmupCompileAllOnlineJoinsEnabled above.
    public boolean isWarmupPeriodicEnabled() {
        return jsonConfig.getBoolean(WARMUP_PERIODIC_ENABLED, false);
    }

    // Milliseconds to block waiting for compile-touched schemas' FastSerde classes to finish generating.
    // 0 (default) means don't wait at all - compiles proceed in the background as before.
    public long getWarmupCompileWaitTimeoutMillis() {
        long seconds = jsonConfig.getLong(WARMUP_COMPILE_WAIT_TIMEOUT_SECONDS, 0L);
        return Math.max(0L, Math.min(seconds, MAX_WARMUP_COMPILE_WAIT_TIMEOUT_SECONDS)) * 1000L;
    }

    public long getWarmupPeriodicIntervalSeconds() {
        long value = jsonConfig.getLong(WARMUP_PERIODIC_INTERVAL_SECONDS, DEFAULT_WARMUP_PERIODIC_INTERVAL_SECONDS);
        return Math.max(value, MIN_WARMUP_PERIODIC_INTERVAL_SECONDS);
    }

    public long getJoinConfTtlMillis() {
        return jsonConfig.getLong(JOIN_CONF_TTL_MILLIS, TTLCache.DefaultTtlMillis());
    }

    public long getJoinCodecTtlMillis() {
        return jsonConfig.getLong(JOIN_CODEC_TTL_MILLIS, TTLCache.DefaultTtlMillis());
    }

    public String encodeConfig() {
        // Redact payload keys — they may contain entity keys that should not be exposed via /config.
        // JsonObject(Map) does not copy the backing map, so copy it ourselves before mutating - otherwise
        // remove() below would delete the key from the live jsonConfig, not a redacted copy of it.
        JsonObject redacted = new JsonObject(new HashMap<>(jsonConfig.getMap()));
        redacted.remove(WARMUP_PAYLOAD);
        for (int i = 0; jsonConfig.getString(String.format(WARMUP_JOIN_INDEXED_REGEX_FMT, i)) != null; i++) {
            redacted.remove(String.format(WARMUP_JOIN_INDEXED_PAYLOAD_FMT, i));
        }
        return redacted.encodePrettily();
    }
}
