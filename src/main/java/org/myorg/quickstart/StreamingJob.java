/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class StreamingJob {
	public static final MapStateDescriptor<String, String> DESCRIPTOR = new MapStateDescriptor<>(
			"RulesBroadcastState",
			BasicTypeInfo.STRING_TYPE_INFO,
			TypeInformation.of(new TypeHint<String>() {
			})
	);

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");

		BroadcastStream<NodeConfigurationSchema> broadcastStream = env
				.addSource(new FlinkKafkaConsumer<>("additional", new KafkaStreamDeserealizationSchema(), properties))
				.flatMap(new BroadcastStreamFilter())
				.broadcast(DESCRIPTOR);

		final DataStreamSource<String> main = env
				.addSource(new FlinkKafkaConsumer<>("main", new SimpleStringSchema(), properties));

		main
				.connect(broadcastStream)
				.process(new NewRateFunc(10))
				//.connect(broadcastStream)
				//.process(new ChangeMultiplierFunc(10))
				.print();



		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
