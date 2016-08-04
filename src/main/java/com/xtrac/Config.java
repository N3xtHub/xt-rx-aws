/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xtrac;

import java.util.Properties;

public class Config {

	private String dbUser;
	private String dbUrl;
	private String dbPassword;
	private String consumerName;
	private String dbo;
	private String eventQ;
	private String proxyHost;
	private int proxyPort;
	private String regionName;
	private String stream;

	public String getDbUser() {
		return dbUser;
	}

	public String getDbUrl() {
		return dbUrl;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public String getConsumerName() {
		return consumerName;
	}

	public String getDbo() {
		return dbo;
	}

	public String getEventQ() {
		return eventQ;
	}

	public String getProxyHost() {
		return proxyHost;
	}

	public int getProxyPort() {
		return proxyPort;
	}

	public String getRegionName() {
		return regionName;
	}

	public String getStream() {
		return stream;
	}

	public Config(Properties props) {
		if (props == null) {
			return;
		}

		dbUser = props.getProperty("dbUser");
		dbo = props.getProperty("dbo");
		dbPassword = props.getProperty("dbPassword");
		consumerName = props.getProperty("consumerName");
		dbUrl = props.getProperty("dbUrl");
		eventQ = props.getProperty("eventQ");

		proxyHost = props.getProperty("proxyHost");

		if (!proxyHost.equals("")) {
			proxyPort = Integer.valueOf(props.getProperty("proxyPort"));
		}
		regionName = props.getProperty("awsRegion");
		stream = props.getProperty("kinesisStreamName");
	}
}
