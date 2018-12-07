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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


/**
 * Simple SinkFunction example which just print record to stdout.
 */
public class SimpleSinkFunction extends AbstractRichFunction implements SinkFunction<Integer> {

	@Override
	public void invoke(Integer value, SinkFunction.Context context) {
		System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " -> " + value);
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).addSink(new SimpleSinkFunction());
		env.execute();
	}
}
