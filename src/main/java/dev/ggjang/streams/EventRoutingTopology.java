package dev.ggjang.streams;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import static dev.ggjang.streams.Models.*;

public final class EventRoutingTopology {
    public static final String INPUT = RuleJoinTopology.OUTPUT;
    public static final String ARCHIVE = "archived-readings";
    public static final String ALERTS = "alert-candidates";
    public static final String MISSING_RULES = "missing-rule-readings";

    private EventRoutingTopology() {}

    private enum Kind { ARCHIVE, ALERT, MISSING_RULE }
    private record RoutedReading(Kind kind, EvaluatedReading reading) {}

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), JsonSerde.of(EvaluatedReading.class)))
                .filter((sensorId, reading) -> sensorId != null && reading != null)
                .flatMap(EventRoutingTopology::createEvents)
                .split(Named.as("route-"))
                .branch((sensorId, event) -> event.kind() == Kind.ARCHIVE,
                        Branched.withConsumer(stream -> write(stream, ARCHIVE), "archive"))
                .branch((sensorId, event) -> event.kind() == Kind.ALERT,
                        Branched.withConsumer(stream -> write(stream, ALERTS), "alerts"))
                .branch((sensorId, event) -> event.kind() == Kind.MISSING_RULE,
                        Branched.withConsumer(stream -> write(stream, MISSING_RULES), "missing-rules"))
                .noDefaultBranch();
        return builder.build();
    }

    private static List<KeyValue<String, RoutedReading>> createEvents(String sensorId, EvaluatedReading reading) {
        List<KeyValue<String, RoutedReading>> events = new ArrayList<>();
        events.add(KeyValue.pair(sensorId, new RoutedReading(Kind.ARCHIVE, reading)));
        switch (reading.severity()) {
            case WARNING, CRITICAL -> events.add(KeyValue.pair(sensorId, new RoutedReading(Kind.ALERT, reading)));
            case NO_RULE -> events.add(KeyValue.pair(sensorId, new RoutedReading(Kind.MISSING_RULE, reading)));
            case NORMAL -> { /* The archive record is sufficient. */ }
        }
        return events;
    }

    private static void write(KStream<String, RoutedReading> stream, String topic) {
        stream.mapValues(RoutedReading::reading)
                .to(topic, Produced.with(Serdes.String(), JsonSerde.of(EvaluatedReading.class)));
    }
}
