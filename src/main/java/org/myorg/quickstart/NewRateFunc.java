package org.myorg.quickstart;

import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class NewRateFunc extends BroadcastProcessFunction<String, String, String> {

  private final long messagesPerSecond;
  private long currentPercentage = 100;
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
    messagesRateLimiter.setRate(messagesPerSecond * currentPercentage / 100);
    messagesRateLimiter.open(getRuntimeContext());
  }

  @Override
  public void processElement(
      String s, ReadOnlyContext readOnlyContext, Collector<String> collector
  ) throws Exception {
    try {
      long value = Long.parseLong(s);
     /* System.out.println(getRuntimeContext().getMapState(new MapStateDescriptor<>(
          "RulesBroadcastState",
          BasicTypeInfo.STRING_TYPE_INFO,
          TypeInformation.of(new TypeHint<String>() {}))));*/
      messagesRateLimiter.acquire(s.length()*100);
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
      currentPercentage = Long.parseLong(s);
      configureRateLimiter();
      //Thread.currentThread().interrupt();
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
