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
import java.util.concurrent.CountDownLatch;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.sqs.model.Message;
import com.xtrac.Config;
import com.xtrac.reactor.aws.sqs.SQSMessage;
import com.xtrac.reactor.aws.sqs.SQSReactorBridge;

import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.registry.Registration;
import reactor.bus.selector.Selectors;

public class SQSMessageTest {

	static Logger log = LoggerFactory.getLogger(SQSMessageTest.class);

	@Test
	public void testIt() throws InterruptedException {
		Message m = new Message();

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
		if (config.getProxyHost() != null && config.getProxyHost() != "") {
			clientConfiguration.setProxyHost(config.getProxyHost());
			clientConfiguration.setProxyPort(config.getProxyPort());
		}

		EventBus b = EventBus.create(Environment.initializeIfEmpty());
		SQSReactorBridge bridge = new SQSReactorBridge.Builder().withRegion(config.getRegionName())
				.withUrl("https://api.example.com").withEventBus(b).build();

		SQSMessage msg = new SQSMessage(bridge, m);

		Assertions.assertThat(msg.getMessage()).isSameAs(m);

		Event<SQSMessage> em = Event.wrap(msg);

		Assertions.assertThat(em.getData()).isNotNull();

		CountDownLatch latch = new CountDownLatch(1);

		Registration reg = b.on(Selectors.$("foo"), it -> {
			latch.countDown();
		});
		b.notify("foo", em);

		latch.await();

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
}
