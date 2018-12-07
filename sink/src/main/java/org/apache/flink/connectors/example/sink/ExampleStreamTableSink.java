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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.Preconditions;

public class ExampleStreamTableSink implements AppendStreamTableSink<Integer> {

	String[] names;
	TypeInformation<?>[] types;

	public ExampleStreamTableSink() {
		names = new String[]{"v"};
		types = new TypeInformation[]{TypeInformation.of(Integer.class)};
	}

	public ExampleStreamTableSink(String[] names, TypeInformation<?>[] types) {
		this.names = names;
		this.types = types;
	}

	@Override
	public void emitDataStream(DataStream<Integer> dataStream) {
		dataStream.addSink(new SimpleAsyncSinkFunction());
	}

	@Override
	public TypeInformation<Integer> getOutputType() {
		return TypeInformation.of(Integer.class);
	}

	@Override
	public String[] getFieldNames() {
		return names;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return types;
	}

	@Override
	public TableSink<Integer> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

		Preconditions.checkArgument(fieldNames.length == 1);
		Preconditions.checkArgument(fieldTypes.length == 1);
		Preconditions.checkArgument(fieldTypes[0].equals(TypeInformation.of(Integer.class)));
		return new ExampleStreamTableSink(fieldNames, fieldTypes);
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		execEnv.setParallelism(2);
		execEnv.enableCheckpointing(3000);
		StreamTableEnvironment env =
				new StreamTableEnvironment(execEnv, new TableConfig());
		env.registerDataStream(
				"Test",
				execEnv.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
				"v");
		env.registerTableSink(
				"Sink",
				new String[]{"field"},
				new TypeInformation<?>[]{TypeInformation.of(Integer.class)},
				new ExampleStreamTableSink());
		env.sqlUpdate("insert into Sink select v from Test");
		env.execEnv().execute();
	}
}
