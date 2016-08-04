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
package com.xtrac.reactor.aws.kinesis;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.google.common.base.Strings;
import com.xtrac.Config;

public abstract class AbstractKinesisIntegrationTest {

	static Logger log = LoggerFactory.getLogger(AbstractKinesisIntegrationTest.class);
	static String streamName = null;
	static boolean kinesisAvailable = false;
	static AmazonKinesisAsyncClient kinesisClient = null;
	static Config config = null;
	static Properties configProps = null;

	public static AmazonKinesisAsyncClient getKinesisClient() {

		if (kinesisClient == null) {

			try {

				configProps = readConfig();
				config = new Config(configProps);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			ClientConfiguration clientConfiguration = new ClientConfiguration();

			if (config.getProxyHost() != null && !config.getProxyHost().equals("")) {
				clientConfiguration.setProxyHost(config.getProxyHost());
				clientConfiguration.setProxyPort(config.getProxyPort());
			}

			kinesisClient = new AmazonKinesisAsyncClient(clientConfiguration);
			Regions region = Regions.fromName(config.getRegionName());
			kinesisClient.setRegion(Region.getRegion(region));

		}
		return kinesisClient;
	}

	private static Properties readConfig() throws FileNotFoundException, IOException {
		String propFilePath = System.getenv("CONFIG_PATH");

		log.info("config path: " + propFilePath);
		if (propFilePath == null) {
			throw new RuntimeException(
					"CONFIG_PATH environment variable not set - cannot read configuration properties");
		}

		File file = new File(propFilePath);
		FileInputStream fileInput = new FileInputStream(file);
		Properties properties = new Properties();
		properties.load(fileInput);
		fileInput.close();

		return properties;
	}

	@Before
	public void checkAvailability() {
		Assume.assumeTrue(kinesisAvailable);
	}

	@BeforeClass
	public static void setup() {

		try {
			AmazonKinesisAsyncClient client = getKinesisClient();

			client.listStreams().getStreamNames().forEach(s -> {
				log.info("existing stream: {}", s);

			});
			boolean streamAvailable = false;
			try {
				com.amazonaws.services.kinesis.model.DescribeStreamResult r = client.describeStream(getStreamName());

				streamAvailable = true;

			} catch (RuntimeException e) {
				log.info("stream unavailable: " + e.toString());
				streamAvailable = false;
			}

			if (streamAvailable == false) {
				log.info("creating stream: {}", getStreamName());
				client.createStream(getStreamName(), 1);
			}

			boolean done = false;
			long t0 = System.currentTimeMillis();
			do {
				com.amazonaws.services.kinesis.model.DescribeStreamResult result = client
						.describeStream(getStreamName());
				String status = result.getStreamDescription().getStreamStatus();
				log.info("stream status: {}", status);
				if ("ACTIVE".equals(status)) {
					streamAvailable = true;
					log.info("stream {} is AVAILABLE", streamName);
					done = true;
				}
				if ("DELETING".equals(status)) {
					streamAvailable = false;
					done = true;
				}
				if ("DELETED".equals(status)) {
					streamAvailable = false;
					done = true;
				}
				if ("CREATING".equals(status)) {
					log.info("waiting for stream {} to become available...", getStreamName());
					try {
						Thread.sleep(5000L);
					} catch (Exception e) {
					}
					;
				}

				if (System.currentTimeMillis() > t0 + TimeUnit.MINUTES.toMillis(2)) {
					done = true;
					streamAvailable = false;
				}
			} while (!done);

			if (streamAvailable) {
				log.info("stream is avaialbale: {}", getStreamName());
			} else {
				log.info("stream is not available: {}", getStreamName());
				log.info("kinesis integration tests will be disabled");
			}
			kinesisAvailable = streamAvailable;
			Assume.assumeTrue(streamAvailable);
		} catch (RuntimeException e) {

			log.warn("could not get stream...integration tests will be disabled: " + e.toString());
			kinesisAvailable = false;

		}

		if (kinesisAvailable) {
			deleteAllOldTestStreams();
		}
		Assume.assumeTrue(kinesisAvailable);
	}

	public static void deleteAllOldTestStreams() {

		Pattern p = Pattern.compile("junit-.*KinesisIntegrationTest.*-(\\d+)");
		boolean hasMoreStreams = true;
		AtomicReference<String> ref = new AtomicReference<String>(null);
		while (hasMoreStreams) {
			ListStreamsRequest request = new ListStreamsRequest();
			if (ref.get() != null) {
				request.setExclusiveStartStreamName(ref.get());
			}
			com.amazonaws.services.kinesis.model.ListStreamsResult result = getKinesisClient().listStreams(request);
			result.getStreamNames().forEach(it -> {
				ref.set(it);
				Matcher m = p.matcher(it);

				if (getStreamName().equals(it)) {
					// ignore current stream
				} else if (m.matches()) {
					log.info("test stream: {}", it);

					long ageInMinutes = TimeUnit.MILLISECONDS
							.toMinutes(Math.abs(System.currentTimeMillis() - Long.parseLong(m.group(1))));

					log.info("stream is {} minutes old", ageInMinutes);
					if (ageInMinutes > 30) {
						log.info("deleting {}", it);
						getKinesisClient().deleteStream(it);
					}

				} else {
					log.info("not a test stream: {}", it);
				}

			});
			hasMoreStreams = result.isHasMoreStreams();
		}
		ListStreamsRequest request;

	}

	public static String getStreamName() {
		try {
			if (streamName != null) {
				return streamName;
			}
			File f = new File("./test-stream.properties");
			if (f.exists()) {
				byte[] b = Files.readAllBytes(f.toPath());
				Properties p = new Properties();
				p.load(new ByteArrayInputStream(b));
				String val = p.getProperty("streamName");
				if (!Strings.isNullOrEmpty(val)) {
					streamName = val;
					return streamName;
				}
			}

			String newStreamName = ("junit-" + AbstractKinesisIntegrationTest.class.getName() + "-"
					+ System.currentTimeMillis()).replace(".", "-");

			Properties p = new Properties();
			p.put("streamName", newStreamName);
			try (FileWriter fw = new FileWriter(f)) {
				p.store(fw, "");
			}
			streamName = newStreamName;
			return streamName;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

}
