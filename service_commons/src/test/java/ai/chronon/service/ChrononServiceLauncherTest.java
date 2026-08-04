package ai.chronon.service;

import ai.chronon.online.metrics.OtelMetricsReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ChrononServiceLauncherTest {

    private static final String CHRONON_RESOURCES_PROP = OtelMetricsReporter.MetricsExporterResourceKey();

    private String savedChrononProp;

    @BeforeEach
    void saveSysProps() {
        savedChrononProp = System.getProperty(CHRONON_RESOURCES_PROP);
        System.clearProperty(CHRONON_RESOURCES_PROP);
    }

    @AfterEach
    void restoreSysProps() {
        if (savedChrononProp == null) {
            System.clearProperty(CHRONON_RESOURCES_PROP);
        } else {
            System.setProperty(CHRONON_RESOURCES_PROP, savedChrononProp);
        }
    }

    private Function<String, String> envOf(Map<String, String> env) {
        return env::get;
    }

    // Mirrors how Micrometer's OtlpConfig parses the resourceAttributes string into a LinkedHashMap
    // where the last value for a duplicate key wins. Asserting against this parsed view keeps the
    // precedence checks meaningful even if the raw string layout changes.
    private Map<String, String> parseAsMicrometerWould(String resourceAttributes) {
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String entry : resourceAttributes.split(",")) {
            String[] kv = entry.split("=", 2);
            if (kv.length == 2) {
                parsed.put(kv[0].trim(), kv[1].trim());
            }
        }
        return parsed;
    }

    @Test
    void onlyDefaultServiceNameWhenNothingElseConfigured() {
        Map<String, String> env = new HashMap<>();
        env.put("HOSTNAME", "fetcher-abc123");

        String result = ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env));

        assertEquals("service.name=ai.chronon,service.instance.id=fetcher-abc123", result);
    }

    @Test
    void allFourSourcesCombineWithCorrectPrecedence() {
        // Both OTEL_RESOURCE_ATTRIBUTES and the chronon sysprop set deployment.environment.name —
        // the sysprop is appended later so it must win. OTEL_SERVICE_NAME is appended last and
        // must beat the default service.name.
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=staging,team=ml");
        env.put("OTEL_SERVICE_NAME", "feature-service-prod");
        System.setProperty(CHRONON_RESOURCES_PROP, "deployment.environment.name=prod,region=us-east-1");

        Map<String, String> parsed = parseAsMicrometerWould(
                ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env)));

        assertEquals("feature-service-prod", parsed.get("service.name"));
        assertEquals("prod", parsed.get("deployment.environment.name"));
        assertEquals("ml", parsed.get("team"));
        assertEquals("us-east-1", parsed.get("region"));
    }

    @Test
    void duplicateServiceNameDoesNotThrow() {
        // Micrometer's OtlpConfig uses Collectors.toMap() which throws on duplicate keys.
        // This test verifies that buildOtlpResourceAttributes deduplicates so the output
        // is safe to pass to Micrometer without triggering IllegalStateException.
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_RESOURCE_ATTRIBUTES", "service.name=ml-default-zipline-hub");

        String result = ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env));

        // OTEL_RESOURCE_ATTRIBUTES wins over the default — last-write-wins
        Map<String, String> parsed = parseAsMicrometerWould(result);
        assertEquals("ml-default-zipline-hub", parsed.get("service.name"));
        // Verify no duplicate keys in the raw string
        long serviceNameCount = java.util.Arrays.stream(result.split(","))
                .filter(s -> s.trim().startsWith("service.name="))
                .count();
        assertEquals(1, serviceNameCount, "Output must not contain duplicate service.name keys");
    }

    @Test
    void otelServiceNameOverridesResourceAttributesServiceName() {
        // OTEL_SERVICE_NAME has highest precedence for service.name
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-resource-attrs");
        env.put("OTEL_SERVICE_NAME", "from-otel-service-name");

        Map<String, String> parsed = parseAsMicrometerWould(
                ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env)));

        assertEquals("from-otel-service-name", parsed.get("service.name"));
    }

    @Test
    void valuesContainingEqualsAreKeptIntact() {
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_RESOURCE_ATTRIBUTES", "key=val=extra");

        Map<String, String> parsed = parseAsMicrometerWould(
                ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env)));

        assertEquals("val=extra", parsed.get("key"));
    }

    @Test
    void entriesWithEmptyKeyOrValueAfterTrimAreRejected() {
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_RESOURCE_ATTRIBUTES", "=value, key= , =,valid=ok");

        Map<String, String> parsed = parseAsMicrometerWould(
                ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env)));

        assertEquals("ok", parsed.get("valid"));
        assertNull(parsed.get(""));
        assertNull(parsed.get("key"));
    }

    @Test
    void blankInputsAreIgnoredWithoutTrailingDelimiters() {
        // Guards against producing strings like "service.name=ai.chronon,," that would parse
        // into spurious empty entries on the receiving side.
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_RESOURCE_ATTRIBUTES", "   ");
        env.put("OTEL_SERVICE_NAME", "");
        env.put("HOSTNAME", "fetcher-abc123");
        System.setProperty(CHRONON_RESOURCES_PROP, "");

        String result = ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env));

        assertEquals("service.name=ai.chronon,service.instance.id=fetcher-abc123", result);
    }

    // CTRL-281: without a per-instance attribute every replica exports an identical resource block
    // and downstream consumers collapse the replicas onto one timeseries (last-writer-wins).
    @Test
    void serviceInstanceIdDefaultsToHostname() {
        Map<String, String> env = new HashMap<>();
        env.put("HOSTNAME", "chronon-fetcher-7d9f8b6c4-xk2mz");

        Map<String, String> parsed = parseAsMicrometerWould(
                ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env)));

        assertEquals("chronon-fetcher-7d9f8b6c4-xk2mz", parsed.get("service.instance.id"));
    }

    @Test
    void explicitServiceInstanceIdBeatsHostnameDefault() {
        // The default is seeded before user config precisely so an explicit value still wins.
        Map<String, String> env = new HashMap<>();
        env.put("HOSTNAME", "pod-from-hostname");
        env.put("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=explicitly-configured");

        String result = ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env));

        assertEquals("explicitly-configured", parseAsMicrometerWould(result).get("service.instance.id"));
        // Micrometer's OtlpConfig throws on duplicate keys, so the override must replace rather than append.
        long instanceIdCount = java.util.Arrays.stream(result.split(","))
                .filter(s -> s.trim().startsWith("service.instance.id="))
                .count();
        assertEquals(1, instanceIdCount, "Output must not contain duplicate service.instance.id keys");
    }

    @Test
    void systemPropertyCanOverrideServiceInstanceId() {
        Map<String, String> env = new HashMap<>();
        env.put("HOSTNAME", "pod-from-hostname");
        System.setProperty(CHRONON_RESOURCES_PROP, "service.instance.id=from-sysprop");

        Map<String, String> parsed = parseAsMicrometerWould(
                ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env)));

        assertEquals("from-sysprop", parsed.get("service.instance.id"));
    }

    @Test
    void serviceInstanceIdIsAlwaysPopulatedWhenHostnameIsBlankOrAbsent() {
        // Falls back to local hostname, then a random UUID — the attribute must never be missing,
        // since an absent value reintroduces the collision this guards against.
        Map<String, String> blankHostname = new HashMap<>();
        blankHostname.put("HOSTNAME", "   ");

        for (Map<String, String> env : java.util.List.of(new HashMap<String, String>(), blankHostname)) {
            String instanceId = parseAsMicrometerWould(
                    ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(env)))
                    .get("service.instance.id");

            assertNotNull(instanceId, "service.instance.id must always be set");
            assertFalse(instanceId.trim().isEmpty(), "service.instance.id must not be blank");
        }
    }

    @Test
    void distinctHostnamesProduceDistinctInstanceIds() {
        // The whole point of the attribute: two replicas must not be identical on the wire.
        Map<String, String> podA = new HashMap<>();
        podA.put("HOSTNAME", "fetcher-pod-a");
        Map<String, String> podB = new HashMap<>();
        podB.put("HOSTNAME", "fetcher-pod-b");

        String a = ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(podA));
        String b = ChrononServiceLauncher.buildOtlpResourceAttributes("ai.chronon", envOf(podB));

        assertNotEquals(a, b);
    }
}
