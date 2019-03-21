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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.FileUtils;

public class AutoScaleResourceCompare {

	public static ObjectMapper mapper = new ObjectMapper();

	public static void main(String[] args) throws IOException {
		String[] jobList = getResourceContent("jobs").split("\n");
		double totalProduct = 0;
		double totalPre = 0;
		for (String jobAndProject : jobList) {
			String[] items = jobAndProject.split(",");
			String projectName = items[0];
			String jobName = items[1];
			double costProduct = getResourceCostFromProduct(projectName, jobName);
			double costPre = getResourceCostFromPre("bayes_team", jobName);
			if (costPre > 0 && costProduct > 0) {
				System.out.println(String.format("%s, %s, %s, %s, %s", projectName, jobName, costProduct, costPre, costPre / costProduct));
				totalPre += costPre;
				totalProduct += costProduct;
			}
		}
		System.out.println(String.format("%s, %s, %s, %s, %s", "total", "total", totalProduct, totalPre, totalPre / totalProduct));
	}

	public static double getResourceCostFromPre(String projectName, String jobName) throws IOException {
		String url = "https://pre-sdp.alibaba-inc.com/inner/v1/project/" + projectName + "/job/" +jobName+ "/instance";
		Map<String, String> headers = new HashMap<>();
		headers.put("accessId", "tesla");
		headers.put("accessKey", "tesla");
		String response = HttpUtils.get(url, headers);
		Instance instance = mapper.readValue(response, Instance.class);
		if (instance != null && instance.data != null) {
			return instance.data.data.slots / 100;
		} else {
			return -1;
		}
	}

	public static double getResourceCostFromProduct(String projectName, String jobName) throws IOException {
		String url = "https://sdp.alibaba-inc.com/inner/v1/project/" + projectName + "/job/" +jobName+ "/instance";
		Map<String, String> headers = new HashMap<>();
		headers.put("accessId", "tesla");
		headers.put("accessKey", "tesla");
		String response = HttpUtils.get(url, headers);
		Instance instance = mapper.readValue(response, Instance.class);
		if (instance != null && instance.data != null) {
			return instance.data.data.slots / 100;
		} else {
			return -1;
		}
	}

	public static File getResource(String relativePath) {
		return new File(AutoScaleResourceCompare.class.getClassLoader().getResource(relativePath).getFile());
	}

	public static String getResourceContent(String relativePath) throws IOException {
		return FileUtils.readFileUtf8(getResource(relativePath));
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	static class Instance {
		public InstanceInfo data;
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	static class InstanceInfo {
		public Detail data;
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	static class Detail {
		public String id;
		public double slots;
	}
}
