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

class StateChangeTopologyTest {
    @TempDir Path stateDir;
    private TopologyTestDriver driver;
    private TestInputTopic<String, EvaluatedReading> input;
    private TestOutputTopic<String, EvaluatedReading> output;

    @BeforeEach void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "changes-test");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(StateChangeTopology.build(), config);
        input = driver.createInputTopic(StateChangeTopology.INPUT,
                Serdes.String().serializer(), JsonSerde.of(EvaluatedReading.class).serializer());
        output = driver.createOutputTopic(StateChangeTopology.OUTPUT,
                Serdes.String().deserializer(), JsonSerde.of(EvaluatedReading.class).deserializer());
    }

    @AfterEach void tearDown() { if (driver != null) driver.close(); }

    @ParameterizedTest
    @EnumSource(Severity.class)
    void emitsFirstReadingForEverySeverity(Severity severity) {
        var reading = new EvaluatedReading(75, severity);
        input.pipeInput("sensor-1", reading);
        assertEquals(List.of(KeyValue.pair("sensor-1", reading)), output.readKeyValuesToList());
    }

    @Test void suppressesRepeatedSeverityButKeepsLatestMeasurementInStore() {
        input.pipeInput("sensor-1", new EvaluatedReading(75, Severity.WARNING));
        output.readValue();
        input.pipeInput("sensor-1", new EvaluatedReading(77, Severity.WARNING));
        assertTrue(output.isEmpty());
        var store = driver.<String, StateChangeTopology.SensorState>getTimestampedKeyValueStore(StateChangeTopology.STORE);
        assertEquals(new StateChangeTopology.SensorState(77, Severity.WARNING, true, false),
                store.get("sensor-1").value());
    }

    @Test void emitsEscalationAndRecoveryInOrder() {
        input.pipeInput("sensor-1", new EvaluatedReading(50, Severity.NORMAL));
        input.pipeInput("sensor-1", new EvaluatedReading(75, Severity.WARNING));
        input.pipeInput("sensor-1", new EvaluatedReading(77, Severity.WARNING));
        input.pipeInput("sensor-1", new EvaluatedReading(90, Severity.CRITICAL));
        input.pipeInput("sensor-1", new EvaluatedReading(50, Severity.NORMAL));
        assertEquals(List.of(
                KeyValue.pair("sensor-1", new EvaluatedReading(50, Severity.NORMAL)),
                KeyValue.pair("sensor-1", new EvaluatedReading(75, Severity.WARNING)),
                KeyValue.pair("sensor-1", new EvaluatedReading(90, Severity.CRITICAL)),
                KeyValue.pair("sensor-1", new EvaluatedReading(50, Severity.NORMAL))), output.readKeyValuesToList());
    }

    @Test void keepsSensorStatesIndependent() {
        input.pipeInput("sensor-1", new EvaluatedReading(75, Severity.WARNING));
        input.pipeInput("sensor-2", new EvaluatedReading(75, Severity.WARNING));
        input.pipeInput("sensor-1", new EvaluatedReading(90, Severity.CRITICAL));
        input.pipeInput("sensor-2", new EvaluatedReading(76, Severity.WARNING));
        assertEquals(List.of(
                KeyValue.pair("sensor-1", new EvaluatedReading(75, Severity.WARNING)),
                KeyValue.pair("sensor-2", new EvaluatedReading(75, Severity.WARNING)),
                KeyValue.pair("sensor-1", new EvaluatedReading(90, Severity.CRITICAL))), output.readKeyValuesToList());
    }

    @Test void detectsRuleRemovalAndReRegistration() {
        input.pipeInput("sensor-1", new EvaluatedReading(75, Severity.WARNING));
        input.pipeInput("sensor-1", new EvaluatedReading(75, Severity.NO_RULE));
        input.pipeInput("sensor-1", new EvaluatedReading(76, Severity.NO_RULE));
        input.pipeInput("sensor-1", new EvaluatedReading(75, Severity.WARNING));
        assertEquals(List.of(
                new EvaluatedReading(75, Severity.WARNING),
                new EvaluatedReading(75, Severity.NO_RULE),
                new EvaluatedReading(75, Severity.WARNING)), output.readValuesToList());
    }

    @Test void nullInputsDoNotEmitOrResetPreviousState() {
        input.pipeInput("sensor-1", new EvaluatedReading(75, Severity.WARNING));
        output.readValue();
        input.pipeInput("sensor-1", (EvaluatedReading) null);
        input.pipeInput(null, new EvaluatedReading(90, Severity.CRITICAL));
        input.pipeInput("sensor-1", new EvaluatedReading(76, Severity.WARNING));
        assertTrue(output.isEmpty());
        input.pipeInput("sensor-1", new EvaluatedReading(50, Severity.NORMAL));
        assertEquals(List.of(KeyValue.pair("sensor-1", new EvaluatedReading(50, Severity.NORMAL))),
                output.readKeyValuesToList());
    }
}
