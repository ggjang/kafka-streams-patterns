package dev.ggjang.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public final class PatternsApplication {
    private PatternsApplication() {}

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: java -jar target/kafka-streams-patterns.jar <join|changes|route>");
        }
        Topology topology = switch (args[0]) {
            case "join" -> RuleJoinTopology.build();
            case "changes" -> StateChangeTopology.build();
            case "route" -> EventRoutingTopology.build();
            default -> throw new IllegalArgumentException("Unknown example: " + args[0] + "; use join, changes or route");
        };
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "patterns-" + args[0] + "-v1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        config.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getenv().getOrDefault("STATE_DIR", ".state"));
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        // Local single-broker tutorial; this is not a production durability setting.
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        KafkaStreams streams = new KafkaStreams(topology, config);
        CountDownLatch stopped = new CountDownLatch(1);
        streams.setStateListener((next, previous) -> {
            if (next == KafkaStreams.State.ERROR || next == KafkaStreams.State.NOT_RUNNING) {
                stopped.countDown();
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            stopped.countDown();
        }));
        System.out.println(topology.describe());
        streams.start();
        stopped.await();
        boolean failed = streams.state() == KafkaStreams.State.ERROR;
        streams.close(Duration.ofSeconds(10));
        if (failed) throw new IllegalStateException("Stream processing failed; inspect the preceding logs");
    }
}
