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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.Preconditions;

public class SimpleExampleStreamTableSink implements RetractStreamTableSink<Integer> {

	private String[] names = new String[]{"v"};
	private TypeInformation<?>[] types = new TypeInformation[]{TypeInformation.of(Integer.class)};

	public SimpleExampleStreamTableSink() {
	}

	public SimpleExampleStreamTableSink(String[] names, TypeInformation<?>[] types) {
		this.names = names;
		this.types = types;
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Integer>> dataStream) {
		dataStream.map(new MapFunction<Tuple2<Boolean,Integer>, String>() {
			@Override
			public String map(Tuple2<Boolean, Integer> recordWithRetract) throws Exception {
				if (recordWithRetract.f0) {
					return "Add " + recordWithRetract.f1;
				} else {
					return "Retract " + recordWithRetract.f1;
				}
			}
		}).print();
	}

	@Override
	public TypeInformation<Integer> getRecordType() {
		return TypeInformation.of(Integer.class);
	}

	@Override
	public TupleTypeInfo<Tuple2<Boolean, Integer>> getOutputType() {
		return new TupleTypeInfo(Types.BOOLEAN, getRecordType());
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
	public TableSink<Tuple2<Boolean, Integer>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		Preconditions.checkArgument(fieldNames.length == 1);
		Preconditions.checkArgument(fieldTypes.length == 1);
		Preconditions.checkArgument(fieldTypes[0].equals(TypeInformation.of(Integer.class)));
		return new SimpleExampleStreamTableSink(fieldNames, fieldTypes);
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment env =
				new StreamTableEnvironment(execEnv, new TableConfig());
		env.registerDataStream(
				"Test",
				execEnv.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
				"v");
		env.registerTableSink(
				"Sink",
				new SimpleExampleStreamTableSink());
		env.sqlUpdate("insert into Sink select v from Test");
		env.execEnv().execute();
	}
}
