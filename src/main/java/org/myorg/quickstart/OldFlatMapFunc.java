package org.myorg.quickstart;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class OldFlatMapFunc extends RichFlatMapFunction<String, String> {

  private final long messagesPerSecond;
  protected GuavaFlinkConnectorRateLimiter messagesRateLimiter;

  public OldFlatMapFunc(long messagesPerSecond) {
    this.messagesPerSecond = messagesPerSecond;
  }

  @Override
  public void open(Configuration conf) {
    messagesRateLimiter = new GuavaFlinkConnectorRateLimiter();
    configureRateLimiters();
  }

  @Override
  public void flatMap(String val, Collector<String> collector) {
    long value = Long.parseLong(val);
    messagesRateLimiter.acquire(2);
    collector.collect(String.valueOf(value));
  }

  void configureRateLimiters() {
      messagesRateLimiter.setRate(messagesPerSecond);
      messagesRateLimiter.open(getRuntimeContext());

  }
}
