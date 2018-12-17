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

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

public class ProcTimeTableSource implements StreamTableSource<Integer>,DefinedProctimeAttribute {

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.builder()
				.field("v", TypeInformation.of(Integer.class))
				.field("ts", TimeIndicatorTypeInfo.PROCTIME_INDICATOR()).build();
	}

	@Override
	public String getProctimeAttribute() {
		return "ts";
	}

	private String name;

	public ProcTimeTableSource(String name) {
		this.name = name;
	}

	@Override
	public DataStream<Integer> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.fromElements(1, 2,3,4,5,6,7,8,9,10).name(name)
				.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer input) throws Exception {
				return input + 10;
			}
		});
	}

	@Override
	public TypeInformation<Integer> getReturnType() {
		return TypeInformation.of(Integer.class);
	}


	@Override
	public String explainSource(){
		return name;
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		execEnv.setParallelism(2);
		execEnv.enableCheckpointing(3000);
		StreamTableEnvironment env =
				new StreamTableEnvironment(execEnv, new TableConfig());
		env.registerTableSource("Test", new ProcTimeTableSource("TestSource"));
		env.toAppendStream(env.sqlQuery("select ts from Test"), Timestamp.class).print();
		env.execEnv().execute();
	}

}
