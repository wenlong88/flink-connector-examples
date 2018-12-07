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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

public class ExampleStreamTableSourceFactory implements StreamTableSourceFactory<Integer> {

	public static final String CUSTOM_KEY = "custom.key";

	@Override
	public StreamTableSource<Integer> createStreamTableSource(Map<String, String> properties) {
		return new ExampleStreamTableSource(properties.get(CUSTOM_KEY));
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> requireContext = new HashMap<>();
		requireContext.put(CONNECTOR_TYPE, "example");
		return requireContext;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new LinkedList<>();
		properties.add(CONNECTOR_TYPE);
		properties.add(CONNECTOR_VERSION);
		properties.add(CONNECTOR_PROPERTY_VERSION);
		properties.add(CUSTOM_KEY);
		return properties;
	}
}
