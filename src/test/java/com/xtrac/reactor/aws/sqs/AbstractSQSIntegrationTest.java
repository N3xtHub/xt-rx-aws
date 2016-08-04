/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xtrac.reactor.aws.sqs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.xtrac.Config;

import ch.qos.logback.core.boolex.Matcher;
import reactor.Environment;
import reactor.bus.EventBus;

public abstract class AbstractSQSIntegrationTest {

	static Logger log = LoggerFactory.getLogger(AbstractSQSIntegrationTest.class);
	static AmazonSQSAsyncClient client;
	static String url;
	static String queueName = "junit-" + AbstractSQSIntegrationTest.class.getName().replace(".", "-") + "-"
			+ System.currentTimeMillis();
	static EventBus bus;
	static Config config = null;
	static Properties configProps = null;

	private static Properties readConfig() throws FileNotFoundException, IOException {
		String propFilePath = System.getenv("CONFIG_PATH");

		log.info("config path: " + propFilePath);
		System.err.println("prop file path is " + propFilePath);
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

	@AfterClass
	public static void cleanup() {
		if (url != null && client != null) {
			log.info("deleting queue: {}", url);
			try {
				client.deleteQueue(url);
			} catch (RuntimeException e) {
				log.warn("problem deleting queue", e);
			}

			if (client != null) {
				ListQueuesResult r = client.listQueues();
				r.getQueueUrls().forEach(x -> {
					cleanupAbandonedTestQueue(x);
				});
			}
		}
	}

	private static void cleanupAbandonedTestQueue(String url) {

		long maxAge = 5;
		Pattern p = Pattern.compile(".*junit.*SQSIntegrationTest-(\\d+)");
		java.util.regex.Matcher m = p.matcher(url);
		if (m.matches()) {
			try {
				long createTime = Long.parseLong(m.group(1));
				long ageInMinutes = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - createTime);
				if (ageInMinutes > maxAge) {
					log.info("deleting {} because it is older than {} minutes", url, maxAge);
					client.deleteQueue(url);
				} else {
					log.info("not deleting {}", url);
				}

			} catch (QueueDoesNotExistException e) {
				// eventual consistency...not a problem
			} catch (RuntimeException e) {

				log.warn("problem deleting queue: " + url, e);
			}
		}

	}

	@BeforeClass
	public static void setup() {
		try {
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
			if (config.getProxyHost() != null && ! config.getProxyHost().equals("")) {
				clientConfiguration.setProxyHost(config.getProxyHost());
				clientConfiguration.setProxyPort(config.getProxyPort());
			}
			Regions region = Regions.fromName(config.getRegionName());

			client = new AmazonSQSAsyncClient(clientConfiguration);
			client.setRegion(Region.getRegion(region));			
			url = client.getQueueUrl(queueName).getQueueUrl();
			log.info("using url: " + url);
		} catch (QueueDoesNotExistException e) {
			log.info("queue does not exist");
			CreateQueueResult r = client.createQueue(queueName);

			try {
				Thread.sleep(5000L);
			} catch (InterruptedException ex) {
			}
			url = r.getQueueUrl();

		} catch (Exception e) {
			log.error("", e);

		}
		bus = EventBus.create(Environment.initializeIfEmpty(), Environment.THREAD_POOL);

		Assume.assumeTrue(client != null && url != null && bus != null);
	}
	
	
	public AmazonSQSAsyncClient getSQSClient() {
		return client;
	}

	public String getQueueUrl() {
		return url;
	}

	public void emptyQueue() {

		ReceiveMessageResult r = getSQSClient().receiveMessage(getQueueUrl());

		while (r.getMessages().size() > 0) {
			r.getMessages().forEach(it -> {
				getSQSClient().deleteMessage(getQueueUrl(), it.getReceiptHandle());
			});

			r = getSQSClient().receiveMessage(getQueueUrl());
		}
		log.info("queue is empty: {}", getQueueUrl());
	}

	public EventBus getEventBus() {
		return bus;
	}
}
