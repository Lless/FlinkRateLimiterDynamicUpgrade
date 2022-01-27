package org.myorg.quickstart;

import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class ChangeMultiplierFunc extends BroadcastProcessFunction<String, String, String> {
  private static final int COMMON_MULTIPLIER = 100;
  private final long bytesPerSecond;
  private int currentMultiplier = COMMON_MULTIPLIER;
  protected GuavaFlinkConnectorRateLimiter messagesRateLimiter;

  public ChangeMultiplierFunc(long bytesPerSecond) {
    this.bytesPerSecond = bytesPerSecond;
  }

  @Override
  public void open(Configuration conf) {
    messagesRateLimiter = new GuavaFlinkConnectorRateLimiter();
    configureRateLimiter();
  }

  private void configureRateLimiter() {
    messagesRateLimiter.setRate(bytesPerSecond * COMMON_MULTIPLIER);
    messagesRateLimiter.open(getRuntimeContext());
  }

  @Override
  public void processElement(
      String s, ReadOnlyContext readOnlyContext, Collector<String> collector
  ) throws Exception {
    try {
      long value = Long.parseLong(s);
      messagesRateLimiter.acquire(s.length() * 100 * currentMultiplier);
      collector.collect(String.valueOf(value));
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Override
  public void processBroadcastElement(
      String s, Context context, Collector<String> collector
  ) throws Exception {
    try {
      System.out.println("additional: "+ s);
      currentMultiplier = (int) Math.ceil(COMMON_MULTIPLIER * 100.0 / Integer.parseInt(s));
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
