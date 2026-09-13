package dev.ggjang.streams;

import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static dev.ggjang.streams.Models.*;
import static org.junit.jupiter.api.Assertions.*;

class RuleJoinTopologyTest {
    @TempDir Path stateDir;
    private TopologyTestDriver driver;
    private TestInputTopic<String, Reading> readings;
    private TestInputTopic<String, Rule> rules;
    private TestOutputTopic<String, EvaluatedReading> output;

    @BeforeEach void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-test");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(RuleJoinTopology.build(), config);
        readings = driver.createInputTopic(RuleJoinTopology.READINGS,
                Serdes.String().serializer(), JsonSerde.of(Reading.class).serializer());
        rules = driver.createInputTopic(RuleJoinTopology.RULES,
                Serdes.String().serializer(), JsonSerde.of(Rule.class).serializer());
        output = driver.createOutputTopic(RuleJoinTopology.OUTPUT,
                Serdes.String().deserializer(), JsonSerde.of(EvaluatedReading.class).deserializer());
    }

    @AfterEach void tearDown() { driver.close(); }

    @Test void appliesThresholdBoundariesAndPreservesSensorKey() {
        rules.pipeInput("sensor-1", new Rule(60, 80));
        readings.pipeInput("sensor-1", new Reading(59));
        readings.pipeInput("sensor-1", new Reading(60));
        readings.pipeInput("sensor-1", new Reading(80));
        var first = output.readKeyValue();
        assertEquals("sensor-1", first.key);
        assertEquals(new EvaluatedReading(59, Severity.NORMAL), first.value);
        assertEquals(new EvaluatedReading(60, Severity.WARNING), output.readValue());
        assertEquals(new EvaluatedReading(80, Severity.CRITICAL), output.readValue());
        assertTrue(output.isEmpty());
    }

    @Test void joinsEachSensorWithItsOwnRule() {
        rules.pipeInput("sensor-1", new Rule(60, 80));
        rules.pipeInput("sensor-2", new Rule(80, 100));
        readings.pipeInput("sensor-1", new Reading(75));
        readings.pipeInput("sensor-2", new Reading(75));
        assertEquals(Severity.WARNING, output.readValue().severity());
        assertEquals(Severity.NORMAL, output.readValue().severity());
    }

    @Test void retainsReadingsWithoutMatchingRules() {
        readings.pipeInput("unknown", new Reading(90));
        assertEquals(new EvaluatedReading(90, Severity.NO_RULE), output.readValue());
    }

    @Test void ruleUpdateOnlyAffectsSubsequentReadings() {
        rules.pipeInput("sensor-1", new Rule(60, 80));
        readings.pipeInput("sensor-1", new Reading(75));
        assertEquals(Severity.WARNING, output.readValue().severity());
        rules.pipeInput("sensor-1", new Rule(80, 100));
        assertTrue(output.isEmpty(), "Updating a table must not re-evaluate past readings");
        readings.pipeInput("sensor-1", new Reading(75));
        assertEquals(Severity.NORMAL, output.readValue().severity());
    }

    @Test void tombstoneRemovesTheRule() {
        rules.pipeInput("sensor-1", new Rule(60, 80));
        rules.pipeInput("sensor-1", (Rule) null);
        assertTrue(output.isEmpty());
        readings.pipeInput("sensor-1", new Reading(75));
        assertEquals(Severity.NO_RULE, output.readValue().severity());
    }

    @Test void ignoresNullReadingAndMissingSensorId() {
        readings.pipeInput("sensor-1", (Reading) null);
        readings.pipeInput(null, new Reading(75));
        assertTrue(output.isEmpty());
    }

    @Test void rejectsInvalidRulesAndIncompleteJson() {
        assertThrows(IllegalArgumentException.class, () -> new Rule(80, 60));
        assertThrows(IllegalArgumentException.class, () -> new Reading(Double.NaN));
        assertThrows(org.apache.kafka.common.errors.SerializationException.class,
                () -> JsonSerde.of(Rule.class).deserializer().deserialize("rules", "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }
}
