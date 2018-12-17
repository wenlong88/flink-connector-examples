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

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

public class EventTimeTableSoure implements StreamTableSource<Integer>,DefinedRowtimeAttributes {
	@Override
	public DataStream<Integer> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.addSource(new EventTimeExample());
	}

	@Override
	public TypeInformation<Integer> getReturnType() {
		return TypeInformation.of(Integer.class);
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.builder()
				.field("v", TypeInformation.of(Integer.class))
				.field("rowTime", TimeIndicatorTypeInfo.ROWTIME_INDICATOR())
				.build();
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		List<RowtimeAttributeDescriptor> descriptors = new LinkedList<>();
		RowtimeAttributeDescriptor rowtimeAttributeDescriptor =
				// 保留datastream里的event time和watermark
				new RowtimeAttributeDescriptor(
						"rowTime",
						new StreamRecordTimestamp(),
						new PreserveWatermarks());
		descriptors.add(rowtimeAttributeDescriptor);
		return descriptors;
	}

	@Override
	public String explainSource() {
		return "1";
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		execEnv.setParallelism(2);
		execEnv.enableCheckpointing(3000);
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment env =
				new StreamTableEnvironment(execEnv, new TableConfig());
		env.registerTableSource("Test", new EventTimeTableSoure());
		env.toAppendStream(env.sqlQuery("select count(v) from Test group by " +
				"Tumble(rowTime, interval '1' second)"), Long.class).print();
		env.execEnv().execute();
	}
}
