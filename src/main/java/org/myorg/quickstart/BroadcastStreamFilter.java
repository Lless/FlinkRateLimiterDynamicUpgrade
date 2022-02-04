package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class BroadcastStreamFilter implements FlatMapFunction<KafkaStreamSchema, NodeConfigurationSchema>
{
  @Override
  public void flatMap(
      KafkaStreamSchema kafkaStreamSchema, Collector<NodeConfigurationSchema> collector
  ) {
    if (!kafkaStreamSchema.job_id.equals("1")) return;
    collector.collect(kafkaStreamSchema.parameters);
  }
}
