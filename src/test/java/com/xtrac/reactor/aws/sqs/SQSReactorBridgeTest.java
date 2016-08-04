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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.xtrac.reactor.aws.sqs.SQSReactorBridge;

import reactor.Environment;
import reactor.bus.EventBus;

public class SQSReactorBridgeTest {

	static EventBus bus = EventBus.create(Environment.initializeIfEmpty(), Environment.THREAD_POOL);

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

	@Test
	public void testBuilderSuccess() {
		ClientConfiguration clientConfiguration = new ClientConfiguration();

		SQSReactorBridge bridge = new SQSReactorBridge.Builder().withRegion("us-west-1").withEventBus(bus)
				.withUrl("https://api.example.com").withClientConfiguration(clientConfiguration).build();
		Assertions.assertThat(bridge).isNotNull();
		Assertions.assertThat(bridge.getFailureCount().get()).isEqualTo(0);
		Assertions.assertThat(bridge.getEventBus()).isSameAs(bus);
		Assertions.assertThat(bridge.getQueueUrl()).isEqualTo("https://api.example.com");
		Assertions.assertThat(bridge.getAsyncClient()).isNotNull();
		Assertions.assertThat(bridge.waitTimeSeconds).isEqualTo(10);

		AmazonSQSAsyncClient sqsClient = new AmazonSQSAsyncClient(new DefaultAWSCredentialsProviderChain());
		bridge = new SQSReactorBridge.Builder().withRegion("us-west-1").withEventBus(bus)
				.withUrl("https://api.example.com").withSQSClient(sqsClient).build();

		Assertions.assertThat(bridge.getAsyncClient()).isNotNull();
		Assertions.assertThat(bridge.getAsyncClient()).isSameAs(sqsClient);
		Assertions.assertThat(bridge.isAutoDeleteEnabled()).isTrue();

	}

}
