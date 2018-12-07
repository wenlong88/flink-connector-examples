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

package org.apache.flink.connectors.example.source;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * Call sequence:
 *    1. initializeState()
 *    2. open()
 *    3.
 */
public class StatefulSource
		extends AbstractRichFunction
		implements ParallelSourceFunction<Integer>, CheckpointedFunction, CheckpointListener {

	private int current;
	private MapStateDescriptor<Integer, Integer> mapStateDescriptor;
	private BroadcastState<Integer, Integer> splitState;
	private int checkpointCount = 0;

	@Override
	public void open(Configuration parameters) throws Exception {
		System.out.println("open");
		int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		if (splitState.contains(subTaskIndex)) {
			current = splitState.get(subTaskIndex);
		} else {
			current = subTaskIndex;
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
		System.out.println("snapshot");
		splitState.clear();
		splitState.put(getRuntimeContext().getIndexOfThisSubtask(), current);
	}



	@Override
	public void initializeState(
			FunctionInitializationContext functionInitializationContext) throws Exception {
		System.out.println("init");
		mapStateDescriptor = new MapStateDescriptor("SplitState", Integer.class, Integer.class);
		splitState = functionInitializationContext.getOperatorStateStore().getBroadcastState(mapStateDescriptor);
	}

	@Override
	public void run(SourceContext sourceContext) throws Exception {
		boolean emitted = false;
		while(current < 10) {
			if (!emitted && checkpointCount == 0) {
				synchronized (sourceContext.getCheckpointLock()) {
					sourceContext.collect(current);
					current += getRuntimeContext().getNumberOfParallelSubtasks();
					emitted = true;
				}
			} else if (checkpointCount > 0) {
				throw new Exception("Trigger failover");
			}
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		System.out.println("cancel");

	}

	@Override
	public void notifyCheckpointComplete(long l) throws Exception {
		System.out.println("checkpoint");
		checkpointCount++;
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		env.addSource(new StatefulSource()).setParallelism(2).print().setParallelism(2);
		env.execute();

	}

}
