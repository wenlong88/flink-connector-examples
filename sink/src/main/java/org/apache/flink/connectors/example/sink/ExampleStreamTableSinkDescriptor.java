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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;

public class ExampleStreamTableSinkDescriptor extends ConnectorDescriptor {

	public ExampleStreamTableSinkDescriptor() {
		super("example", 1, false);
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		Map<String, String> properties = new HashMap<>();
		return properties;
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
		env.connect(new ExampleStreamTableSinkDescriptor())
				.registerTableSink("Sink");
		env.sqlUpdate("insert into Sink select v from Test");
		env.execEnv().execute();
	}
}
