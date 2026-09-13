package dev.ggjang.streams;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class JsonSerde {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES);

    private JsonSerde() {}

    public static <T> Serde<T> of(Class<T> type) {
        return Serdes.serdeFrom((topic, value) -> {
            if (value == null) return null; // Preserve Kafka tombstones, not JSON "null".
            try {
                return MAPPER.writeValueAsBytes(value);
            } catch (Exception e) {
                throw new SerializationException("Cannot serialize " + type.getSimpleName(), e);
            }
        }, (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return MAPPER.readValue(bytes, type);
            } catch (Exception e) {
                throw new SerializationException("Cannot deserialize " + type.getSimpleName(), e);
            }
        });
    }
}
