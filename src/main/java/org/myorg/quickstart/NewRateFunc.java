package org.myorg.quickstart;

import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class NewRateFunc extends BroadcastProcessFunction<String, String, String> {

  private final long messagesPerSecond;
  private String currentPercentage = "100";
  protected GuavaFlinkConnectorRateLimiter messagesRateLimiter;

  public NewRateFunc(long messagesPerSecond) {
    this.messagesPerSecond = messagesPerSecond;
  }

  @Override
  public void open(Configuration conf) {
    messagesRateLimiter = new GuavaFlinkConnectorRateLimiter();
    configureRateLimiter();
  }

  private void configureRateLimiter() {
    messagesRateLimiter.setRate(messagesPerSecond * Integer.parseInt(currentPercentage) / 100);
    messagesRateLimiter.open(getRuntimeContext());
  }

  @Override
  public void processElement(
      String s, ReadOnlyContext readOnlyContext, Collector<String> collector
  ) throws Exception {
    try {

      final ReadOnlyBroadcastState<String, String> broadcastState =
          readOnlyContext.getBroadcastState(StreamingJob.DESCRIPTOR);
      if (broadcastState.contains("percentage") && !broadcastState.get("percentage").equals(currentPercentage)) {
        currentPercentage = broadcastState.get("percentage");
        configureRateLimiter();
      }

      messagesRateLimiter.acquire(s.length() * 100);
      System.out.print(currentPercentage);
      collector.collect(s);

    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Override
  public void processBroadcastElement(
      String s, Context context, Collector<String> collector
  ) throws Exception {
    try {
      //System.out.println("additional: " + s);
      final BroadcastState<String, String> broadcastState = context.getBroadcastState(StreamingJob.DESCRIPTOR);
      broadcastState.put("percentage", s);
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
