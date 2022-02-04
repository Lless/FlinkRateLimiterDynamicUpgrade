package org.myorg.quickstart;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

public class KafkaStreamDeserealizationSchema implements DeserializationSchema<KafkaStreamSchema> {
  private static final long serialVersionUID = -1L;

  private final ObjectMapper mapper = new ObjectMapper();


  @Override
  public KafkaStreamSchema deserialize(byte[] bytes) throws IOException {
    if (bytes == null) {
      return null;
    }
    final KafkaStreamSchema kafkaStreamSchema = mapper.readValue(bytes, KafkaStreamSchema.class);
    System.out.println(kafkaStreamSchema);
    return kafkaStreamSchema;
  }

  @Override
  public boolean isEndOfStream(KafkaStreamSchema kafkaStreamSchema) {
    return false;
  }

  @Override
  public TypeInformation<KafkaStreamSchema> getProducedType() {
    return getForClass(KafkaStreamSchema.class);
  }
}
