package dev.ggjang.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Produced;

import static dev.ggjang.streams.Models.*;

public final class RuleJoinTopology {
    public static final String READINGS = "sensor-readings";
    public static final String RULES = "sensor-rules";
    public static final String OUTPUT = "evaluated-readings";

    private RuleJoinTopology() {}

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<String, Rule> rules = builder.globalTable(
                RULES, Consumed.with(Serdes.String(), JsonSerde.of(Rule.class)));

        builder.stream(READINGS, Consumed.with(Serdes.String(), JsonSerde.of(Reading.class)))
                .filter((sensorId, reading) -> sensorId != null && reading != null)
                .leftJoin(rules,
                        (sensorId, reading) -> sensorId,
                        (reading, rule) -> new EvaluatedReading(reading.temperature(),
                                rule == null ? Severity.NO_RULE : rule.evaluate(reading.temperature())))
                .to(OUTPUT, Produced.with(Serdes.String(), JsonSerde.of(EvaluatedReading.class)));
        return builder.build();
    }
}
