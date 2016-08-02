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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.model.Message;
import com.google.common.collect.Lists;
import com.xtrac.Config;
import com.xtrac.reactor.aws.sqs.SQSMessage;
import com.xtrac.reactor.aws.sqs.SQSReactorBridge;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import reactor.bus.Event;
import reactor.bus.selector.Selectors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SQSReactorBridgeIntegrationTest extends AbstractSQSIntegrationTest {
	
	private static Properties readConfig() throws FileNotFoundException, IOException {
        String propFilePath = System.getenv("CONFIG_PATH");
        
        log.info("config path: " + propFilePath);
        System.err.println("prop file path is " + propFilePath);
        if(propFilePath == null) {
            throw new RuntimeException("CONFIG_PATH environment variable not set - cannot read configuration properties");
        }

        File file = new File(propFilePath);
        FileInputStream fileInput = new FileInputStream(file);
        Properties properties = new Properties();
        properties.load(fileInput);
        fileInput.close();

        return properties;
    }
	
    @Test
    public void testIt() throws InterruptedException {
        emptyQueue();
        
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
	        clientConfiguration.setProxyHost(config.getProxyHost());
	        clientConfiguration.setProxyPort(config.getProxyPort());
	        
        SQSReactorBridge b = new SQSReactorBridge.Builder()
                .withSQSClient(getSQSClient())
                .withEventBus(getEventBus())
                .withUrl(getQueueUrl())
                .withClientConfiguration(clientConfiguration)
                .withRegion(config.getRegionName())
                .build()
                .start();

        Assertions.assertThat(b.getQueueArn()).startsWith("arn:aws:sqs:");
        CountDownLatch latch = new CountDownLatch(3);
        List<Event<SQSMessage>> list = Lists.newCopyOnWriteArrayList();

        bus.on(Selectors.T(SQSMessage.class), (Event<SQSMessage> evt) -> {
            log.info("Received: {}", evt);
            list.add(evt);
            latch.countDown();

        });

        getSQSClient().sendMessage(getQueueUrl(), "test1");
        getSQSClient().sendMessage(getQueueUrl(), "test2");
        getSQSClient().sendMessage(getQueueUrl(), "test3");
        Assertions.assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
        log.info("received all: {}", list.size());
        list.forEach(evt -> {

            SQSMessage msg = evt.getData();
            Assertions.assertThat(msg).isNotNull();
            Assertions.assertThat(msg.getUrl()).isEqualTo(b.getQueueUrl());
            Assertions.assertThat(msg.getBridge()).isSameAs(b);
            Assertions.assertThat(msg.getArn()).isEqualTo(b.getQueueArn());

            Message sm = msg.getMessage();

            Assertions.assertThat(sm).isNotNull();
            Assertions.assertThat(sm.getAttributes()).hasSize(0);
        });
    }
}
