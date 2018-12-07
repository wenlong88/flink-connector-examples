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

package org.apache.flink.connectors.example.sink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Example sink functions which flush data in batch or while checkpointing.
 */
public class SimpleAsyncSinkFunction
		extends AbstractRichFunction implements SinkFunction<Integer>, CheckpointedFunction {

	private List<Integer> cache = new ArrayList<>();

	@Override
	public void invoke(Integer value, Context context) throws Exception {
		cache.add(value);
		if (cache.size() >= 3) {
			flush();
		}
	}

	private void flush() {
		for (Integer value : cache) {
			System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " -> " + value);
		}
		cache.clear();
	}

	@Override
	public void close() {
		flush();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
		flush();
	}

	@Override
	public void initializeState(
			FunctionInitializationContext functionInitializationContext) throws Exception {

	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).addSink(new SimpleSinkFunction());
		env.execute();
	}
}
