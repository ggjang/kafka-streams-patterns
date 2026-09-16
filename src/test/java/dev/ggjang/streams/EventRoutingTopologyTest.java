package dev.ggjang.streams;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static dev.ggjang.streams.Models.*;
import static org.junit.jupiter.api.Assertions.*;

class EventRoutingTopologyTest {
    @TempDir Path stateDir;
    private TopologyTestDriver driver;
    private TestInputTopic<String, EvaluatedReading> input;
    private TestOutputTopic<String, EvaluatedReading> archive;
    private TestOutputTopic<String, EvaluatedReading> alerts;
    private TestOutputTopic<String, EvaluatedReading> missingRules;

    @BeforeEach void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "route-test");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(EventRoutingTopology.build(), config);
        input = driver.createInputTopic(EventRoutingTopology.INPUT,
                Serdes.String().serializer(), JsonSerde.of(EvaluatedReading.class).serializer());
        archive = output(EventRoutingTopology.ARCHIVE);
        alerts = output(EventRoutingTopology.ALERTS);
        missingRules = output(EventRoutingTopology.MISSING_RULES);
    }

    private TestOutputTopic<String, EvaluatedReading> output(String topic) {
        return driver.createOutputTopic(topic, Serdes.String().deserializer(),
                JsonSerde.of(EvaluatedReading.class).deserializer());
    }

    @AfterEach void tearDown() { if (driver != null) driver.close(); }

    @ParameterizedTest
    @EnumSource(Severity.class)
    void archivesEveryReadingAndRoutesOnlyToItsIntendedDestination(Severity severity) {
        var record = KeyValue.pair("sensor-1", new EvaluatedReading(75, severity));
        input.pipeInput(record.key, record.value);
        assertEquals(List.of(record), archive.readKeyValuesToList());
        assertEquals(severity == Severity.WARNING || severity == Severity.CRITICAL ? List.of(record) : List.of(),
                alerts.readKeyValuesToList());
        assertEquals(severity == Severity.NO_RULE ? List.of(record) : List.of(), missingRules.readKeyValuesToList());
    }

    @Test void preservesRepeatedEventsAndKeysWithoutStatefulDeduplication() {
        var first = KeyValue.pair("sensor-1", new EvaluatedReading(75, Severity.WARNING));
        var second = KeyValue.pair("sensor-2", new EvaluatedReading(90, Severity.CRITICAL));
        input.pipeInput(first.key, first.value);
        input.pipeInput(second.key, second.value);
        input.pipeInput(first.key, first.value);
        assertEquals(List.of(first, second, first), archive.readKeyValuesToList());
        assertEquals(List.of(first, second, first), alerts.readKeyValuesToList());
        assertTrue(missingRules.isEmpty());
    }

    @Test void dropsNullKeyAndNullValueFromEveryDestination() {
        input.pipeInput(null, new EvaluatedReading(90, Severity.CRITICAL));
        input.pipeInput("sensor-1", (EvaluatedReading) null);
        assertTrue(archive.isEmpty());
        assertTrue(alerts.isEmpty());
        assertTrue(missingRules.isEmpty());
    }
}
