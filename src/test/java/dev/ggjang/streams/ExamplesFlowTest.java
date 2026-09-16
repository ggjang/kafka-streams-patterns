package dev.ggjang.streams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static dev.ggjang.streams.Models.*;
import static org.junit.jupiter.api.Assertions.*;

class ExamplesFlowTest {
    @TempDir Path stateDir;

    private TopologyTestDriver driver(String name, Topology topology) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, name);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.resolve(name).toString());
        return new TopologyTestDriver(topology, config);
    }

    private List<KeyValue<String, EvaluatedReading>> output(TopologyTestDriver driver, String topic) {
        return driver.createOutputTopic(topic, Serdes.String().deserializer(),
                JsonSerde.of(EvaluatedReading.class).deserializer()).readKeyValuesToList();
    }

    @Test void sampleFileProducesTheDocumentedChangesAndRoutes() throws Exception {
        try (var changes = driver("sample-changes", StateChangeTopology.build());
             var routes = driver("sample-routes", EventRoutingTopology.build())) {
            var changeInput = changes.createInputTopic(StateChangeTopology.INPUT,
                    Serdes.String().serializer(), Serdes.String().serializer());
            var routeInput = routes.createInputTopic(EventRoutingTopology.INPUT,
                    Serdes.String().serializer(), Serdes.String().serializer());
            for (String line : Files.readAllLines(Path.of("samples/evaluated-readings.txt"))) {
                String[] parts = line.split("\\|", 2);
                changeInput.pipeInput(parts[0], parts[1]);
                routeInput.pipeInput(parts[0], parts[1]);
            }
            var normal = KeyValue.pair("sensor-1", new EvaluatedReading(50, Severity.NORMAL));
            var warning = KeyValue.pair("sensor-1", new EvaluatedReading(75, Severity.WARNING));
            var repeat = KeyValue.pair("sensor-1", new EvaluatedReading(77, Severity.WARNING));
            var critical = KeyValue.pair("sensor-1", new EvaluatedReading(90, Severity.CRITICAL));
            var missing = KeyValue.pair("sensor-2", new EvaluatedReading(75, Severity.NO_RULE));
            var missingRepeat = KeyValue.pair("sensor-2", new EvaluatedReading(76, Severity.NO_RULE));
            var registered = KeyValue.pair("sensor-2", new EvaluatedReading(75, Severity.WARNING));
            assertEquals(List.of(normal, warning, critical, normal, missing, registered),
                    output(changes, StateChangeTopology.OUTPUT));
            assertEquals(List.of(normal, warning, repeat, critical, normal, missing, missingRepeat, registered),
                    output(routes, EventRoutingTopology.ARCHIVE));
            assertEquals(List.of(warning, repeat, critical, registered), output(routes, EventRoutingTopology.ALERTS));
            assertEquals(List.of(missing, missingRepeat), output(routes, EventRoutingTopology.MISSING_RULES));
        }
    }

    @Test void joinOutputFeedsBothExamplesThroughSerializedRecords() {
        try (var join = driver("flow-join", RuleJoinTopology.build());
             var changes = driver("flow-changes", StateChangeTopology.build());
             var routes = driver("flow-routes", EventRoutingTopology.build())) {
            var rules = join.createInputTopic(RuleJoinTopology.RULES,
                    Serdes.String().serializer(), JsonSerde.of(Rule.class).serializer());
            var readings = join.createInputTopic(RuleJoinTopology.READINGS,
                    Serdes.String().serializer(), JsonSerde.of(Reading.class).serializer());
            rules.pipeInput("sensor-1", new Rule(60, 80));
            readings.pipeInput("sensor-1", new Reading(75));
            readings.pipeInput("sensor-1", new Reading(77));
            rules.pipeInput("sensor-1", (Rule) null);
            readings.pipeInput("sensor-1", new Reading(77));
            var joined = join.createOutputTopic(RuleJoinTopology.OUTPUT,
                    Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
            var changeInput = changes.createInputTopic(StateChangeTopology.INPUT,
                    Serdes.String().serializer(), Serdes.ByteArray().serializer());
            var routeInput = routes.createInputTopic(EventRoutingTopology.INPUT,
                    Serdes.String().serializer(), Serdes.ByteArray().serializer());
            for (var record : joined.readKeyValuesToList()) {
                changeInput.pipeInput(record.key, record.value);
                routeInput.pipeInput(record.key, record.value);
            }
            var first = KeyValue.pair("sensor-1", new EvaluatedReading(75, Severity.WARNING));
            var repeat = KeyValue.pair("sensor-1", new EvaluatedReading(77, Severity.WARNING));
            var missing = KeyValue.pair("sensor-1", new EvaluatedReading(77, Severity.NO_RULE));
            assertEquals(List.of(first, missing), output(changes, StateChangeTopology.OUTPUT));
            assertEquals(List.of(first, repeat, missing), output(routes, EventRoutingTopology.ARCHIVE));
            assertEquals(List.of(first, repeat), output(routes, EventRoutingTopology.ALERTS));
            assertEquals(List.of(missing), output(routes, EventRoutingTopology.MISSING_RULES));
        }
    }
}
