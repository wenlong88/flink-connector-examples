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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class InitConnectionExample extends RichSinkFunction<Integer> {

	private DBConnection connection;

	@Override
	public void open(Configuration config) {
		connection = DBConnection.create();
	}

	@Override
	public void invoke(Integer value, SinkFunction.Context context) throws Exception {
		connection.write(value);
	}


	/**
	 * Mock DB connection.
	 */
	private static class DBConnection {
		 static DBConnection create() {
		 	return null;
		 }

		 boolean isFinished() {
		 	return false;
		 }

		 boolean hasNewData() {
			return false;
		 }

		 Integer getNext() {
		 	return 0;
		 }

		 void write(Integer value) {

		 }

	}
}
