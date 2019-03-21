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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HttpUtils {
	private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);
	private static final String DEFAULT_ENCODING = "utf-8";
	private static final int DEFAULT_CONNECT_TIMEOUT = 3000;
	private static final int DEFAULT_SOCKET_TIMEOUT = 10000;
	private static final int METRICS_FETCH_RETRY_NUM = 3;

	public static String join(String parent, String path) throws MalformedURLException {
		if (!parent.startsWith("http://")) {
			LOG.warn("No http:// protocol header found, added automatically");
			parent = "http://" + parent;
		}
		return new URL(new URL(parent), path).toString();
	}

	public static HttpClient getDefaultHttpClient(String url, String userAgent, int connectTimeout, int socketTimeout) {
		LOG.info("Request url:" + url);
		HttpClient client = new DefaultHttpClient();
		client.getParams().setParameter(
			CoreConnectionPNames.CONNECTION_TIMEOUT,
			connectTimeout);
		client.getParams().setParameter(
			CoreConnectionPNames.SO_TIMEOUT,
			socketTimeout);
		if (userAgent != null && !userAgent.isEmpty()) {
			client.getParams().setParameter(CoreProtocolPNames.USER_AGENT, userAgent);
		}
		return client;
	}

	public static String get(String url) {
		return get(url, DEFAULT_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, new HashMap<String, String>());
	}

	public static String get(String url, Map<String, String> headers) {
		return get(url, DEFAULT_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, headers);
	}

	public static String get(String url, int connectTimeout, int socketTimeout, Map<String, String> headers) {
		HttpGet get = new HttpGet(url);
		for (Map.Entry<String, String> entry : headers.entrySet()) {
			get.addHeader(entry.getKey(), entry.getValue());
		}
		HttpClient client = getDefaultHttpClient(url, null, connectTimeout, socketTimeout);

		LOG.info("Executing GET to {}", url);

		try {
			return request(client, get).getValue();
		} catch (Exception e) {
			LOG.error("can't visit url '" + url + "'", e);
		} finally {
			client.getConnectionManager().shutdown();
		}
		return null;
	}

	public static Pair<Integer, String> request(HttpClient client, HttpUriRequest request) throws InterruptedException, IOException {
		for (int i = 0; i < METRICS_FETCH_RETRY_NUM; i++) {
			try {
				HttpResponse response = client.execute(request);
				LOG.info("HttpResponse: " + response);

				String responseBody = IOUtils.toString(response.getEntity().getContent());
				LOG.info("HttpResponse body: {}", responseBody);

				StatusLine statusLine = response.getStatusLine();
				if (HttpStatus.SC_OK == statusLine.getStatusCode()) {
					return Pair.of(statusLine.getStatusCode(), responseBody);
				} else {
					LOG.error("Request failed with status code '{}' and reason '{}'", statusLine.getStatusCode(), statusLine.getReasonPhrase());
					throw new IOException("HttpRequest failed with code response: " + responseBody);
					}
			} catch (IOException e) {
				if (i == METRICS_FETCH_RETRY_NUM - 1) {
					throw e;
				} else {
					Thread.sleep(3000);
					LOG.warn("Failed getting response from server. Retrying.", e);
				}
			}
		}

		throw new IllegalStateException("Shouldn't reach here");
	}

	// will use default user-agent
	public static String post(String url, List<NameValuePair> kvs) throws InterruptedException, IOException {
		return post(url, kvs, null);
	}

	public static String post(String url, List<NameValuePair> kvs, String userAgent) throws InterruptedException, IOException {
		return post(url, new UrlEncodedFormEntity(kvs), userAgent);
	}

	public static String post(String url, String body) throws InterruptedException, IOException {
		return post(url, body, null);
	}

	public static String post(String url, String body, String userAgent) throws InterruptedException, IOException {
		return post(url, new StringEntity(body, DEFAULT_ENCODING), userAgent);
	}

	public static String post(String url, HttpEntity entity, String userAgent) throws InterruptedException, IOException {
		HttpPost post = new HttpPost(url);
		HttpClient client =
			getDefaultHttpClient(url, userAgent, DEFAULT_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
		post.setEntity(entity);

		LOG.info("Executing POST to {} with entity {}", url, IOUtils.toString(post.getEntity().getContent()));

		return request(client, post).getRight();
	}

}
