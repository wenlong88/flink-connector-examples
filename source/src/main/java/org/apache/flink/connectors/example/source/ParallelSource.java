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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * Parallel source example which emit integers from 0 to 10.
 */
public class ParallelSource extends AbstractRichFunction implements ParallelSourceFunction<Integer> {

	@Override
	public void run(SourceContext<Integer> sourceContext) throws Exception {
		int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		int taskCount = getRuntimeContext().getNumberOfParallelSubtasks();
		for (int i = subTaskIndex; i < 10; i += taskCount) {
			sourceContext.collect(i);
		}
	}

	@Override
	public void cancel() {

	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(new ParallelSource()).setParallelism(2).print().setParallelism(2);
		env.execute();
	}
}
