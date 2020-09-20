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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import org.junit.Test;

/**
 * Tests for blocking shuffle.
 */
public class BlockingShuffleITCase {

	private static final String RECORD = "hello, world!";

	private final int numTaskManagers = 2;

	private final int numSlotsPerTaskManager = 4;

	@Test
	public void testBoundedBlockingShuffle() throws Exception {
		JobGraph jobGraph = createJobGraph();
		JobGraphRunningUtil.execute(jobGraph, new Configuration(), numTaskManagers, numSlotsPerTaskManager);
	}

	@Test
	public void testSortMergeBlockingShuffle() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setBoolean(NettyShuffleEnvironmentOptions.NETWORK_SORT_MERGE_SHUFFLE_ENABLED, true);
		configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_SORT_MERGE_SHUFFLE_MIN_PARALLELISM, 1);
		configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_SORT_MERGE_PARTITION, 100);

		JobGraph jobGraph = createJobGraph();
		JobGraphRunningUtil.execute(jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
	}

	private JobGraph createJobGraph() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(numTaskManagers * numSlotsPerTaskManager);
		DataStream<String> source = env.addSource(new StringSource());
		source
			.rebalance().map((MapFunction<String, String>) value -> value)
			.broadcast().addSink(new VerifySink());

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setGlobalDataExchangeMode(GlobalDataExchangeMode.ALL_EDGES_BLOCKING);
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);
		return StreamingJobGraphGenerator.createJobGraph(streamGraph);
	}

	private static class StringSource implements ParallelSourceFunction<String> {
		private volatile boolean isRunning = true;
		private int numRecordsToSend = 1000000;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (isRunning && numRecordsToSend-- > 0) {
				ctx.collect(RECORD);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class VerifySink implements SinkFunction<String> {

		@Override
		public void invoke(String value) throws Exception {
			assert value.equals(RECORD);
		}
	}
}
