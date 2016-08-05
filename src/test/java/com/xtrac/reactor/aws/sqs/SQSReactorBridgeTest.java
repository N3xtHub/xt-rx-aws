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
package com.xtrac.reactor.aws.sqs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.xtrac.Config;
import com.xtrac.reactor.aws.sns.SNSTest;
import com.xtrac.reactor.aws.sqs.SQSReactorBridge;

import reactor.Environment;
import reactor.bus.EventBus;

public class SQSReactorBridgeTest {

	static EventBus bus = EventBus.create(Environment.initializeIfEmpty(), Environment.THREAD_POOL);
	final static Log log = LogFactory.getLog(SNSTest.class);
	
	@Test
	public void testFailedBuilder() {

		try {
			new SQSReactorBridge.Builder().build();
			Assertions.failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
		} catch (Exception e) {
			Assertions.assertThat(e).isInstanceOf(IllegalArgumentException.class);
		}

		try {
			new SQSReactorBridge.Builder().withEventBus(bus).build();
			Assertions.failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
		} catch (Exception e) {
			Assertions.assertThat(e).isInstanceOf(IllegalArgumentException.class);
		}

		try {
			new SQSReactorBridge.Builder().withUrl("https://example.com").build();
			Assertions.failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
		} catch (Exception e) {
			Assertions.assertThat(e).isInstanceOf(IllegalArgumentException.class);
		}
	}
	
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
	
	@Test
	public void testBuilderSuccess() {
		Properties configProps = null;
		try {
			configProps = readConfig();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Config config = new Config(configProps);

		ClientConfiguration clientConfiguration = new ClientConfiguration();
		if (config.getProxyHost() != null && !config.getProxyHost().equals("")) {
			clientConfiguration.setProxyHost(config.getProxyHost());
			clientConfiguration.setProxyPort(config.getProxyPort());
		}
		Regions region = Regions.fromName(config.getRegionName());
		
		AmazonSQSAsyncClient client = new AmazonSQSAsyncClient(clientConfiguration);
		client.setRegion(Region.getRegion(region));
		CreateQueueRequest request = new CreateQueueRequest("test");
		CreateQueueResult result = client.createQueue(request);
		String sqsName = result.getQueueUrl();
		
		SQSReactorBridge bridge = new SQSReactorBridge.Builder().withRegion(config.getRegionName()).withEventBus(bus)
				.withUrl(config.getRegionName()).withClientConfiguration(clientConfiguration).build();
		Assertions.assertThat(bridge).isNotNull();
		Assertions.assertThat(bridge.getFailureCount().get()).isEqualTo(0);
		Assertions.assertThat(bridge.getEventBus()).isSameAs(bus);
		Assertions.assertThat(bridge.getQueueUrl()).isEqualTo(sqsName);
		Assertions.assertThat(bridge.getAsyncClient()).isNotNull();
		Assertions.assertThat(bridge.waitTimeSeconds).isEqualTo(10);

		AmazonSQSAsyncClient sqsClient = new AmazonSQSAsyncClient(new DefaultAWSCredentialsProviderChain());
		bridge = new SQSReactorBridge.Builder().withRegion(config.getRegionName()).withEventBus(bus)
				.withUrl(sqsName).withSQSClient(sqsClient).build();

		Assertions.assertThat(bridge.getAsyncClient()).isNotNull();
		Assertions.assertThat(bridge.getAsyncClient()).isSameAs(sqsClient);
		Assertions.assertThat(bridge.isAutoDeleteEnabled()).isTrue();

	}

}
