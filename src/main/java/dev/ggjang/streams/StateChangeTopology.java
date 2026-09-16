package dev.ggjang.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import static dev.ggjang.streams.Models.*;

public final class StateChangeTopology {
    public static final String INPUT = RuleJoinTopology.OUTPUT;
    public static final String OUTPUT = "sensor-state-changes";
    public static final String STORE = "sensor-severity-state";

    private StateChangeTopology() {}

    // initialized distinguishes the first reading from an actual NO_RULE state.
    public record SensorState(double temperature, Severity severity, boolean initialized, boolean changed) {}

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), JsonSerde.of(EvaluatedReading.class)))
                .filter((sensorId, reading) -> sensorId != null && reading != null)
                .groupByKey(Grouped.with(Serdes.String(), JsonSerde.of(EvaluatedReading.class)))
                .aggregate(
                        () -> new SensorState(0, Severity.NO_RULE, false, false),
                        (sensorId, reading, previous) -> new SensorState(
                                reading.temperature(), reading.severity(), true,
                                !previous.initialized() || previous.severity() != reading.severity()),
                        Materialized.<String, SensorState, KeyValueStore<Bytes, byte[]>>as(STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerde.of(SensorState.class))
                                // Forward each transition instead of coalescing consecutive updates.
                                .withCachingDisabled())
                .toStream()
                .filter((sensorId, state) -> state.changed())
                .mapValues(state -> new EvaluatedReading(state.temperature(), state.severity()))
                .to(OUTPUT, Produced.with(Serdes.String(), JsonSerde.of(EvaluatedReading.class)));
        return builder.build();
    }
}
