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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.xtrac.reactor.aws.kinesis.KinesisReactorBridge;
import com.xtrac.reactor.aws.kinesis.KinesisReactorBridge.KinesisRecord;

import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selectors;

public class KinesisReactorBridgeIntegrationTest extends AbstractKinesisIntegrationTest {

	@Test
	public void testIt() throws InterruptedException {
		Assume.assumeTrue(kinesisAvailable);
		
		
		EventBus bus = EventBus.create(Environment.initializeIfEmpty());
		
		Regions region = Regions.fromName(config.getRegionName());
		
		KinesisReactorBridge bridge = new KinesisReactorBridge.Builder().withRegion(region).withAppName("workflow-runtime")
				.withEventBus(bus).withStreamName(this.getStreamName()).withAdditionalConfig(c -> {
					c.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
				}).build().start();

		CountDownLatch latch = new CountDownLatch(1);
		
		String message = "{\"workItem\":{\"fileNumber\":\"W026938-02AUG16\",\"orgId\":1021,\"itemType\":\"Project Request\",\"status\":\"APRQ\",\"statusType\":\"P\",\"statusDate\":\"2016-08-02T18:47:23-0500\",\"suspenseStatus\":\"A\",\"splitStatus\":\"N\",\"memo\":\" \",\"amount\":0,\"currentQueue\":\"A045103\",\"currentNode\":\"Management\",\"nodeAccessGroup\":\"Management\",\"creatingOperator\":\"A045103\",\"nodeWhereCreated\":\"Management\",\"createDate\":\"2016-08-02T18:47:23-0500\",\"lastEvent\":\"ENTER\",\"lastEventDate\":\"2016-08-02T18:47:23-0500\",\"priorityNumber\":0,\"suspensionCount\":0,\"suspensionDuration\":0,\"queueType\":1,\"queueEnterTime\":\"2016-08-02T18:47:23-0500\",\"parentFileNumber\":0,\"nonCntrSuspensionCount\":0,\"nonCntrSuspensionDuration\":0,\"familyId\":26938080216,\"modOperId\":\"A045103\",\"noteCount\":0,\"parties\":[{\"partyNumber\":1,\"name\":\"ORIG\",\"taxReportingCode\":\" \",\"organizationName\":\"ACME\",\"accountNumber\":\"111\"}]},\"eventDetail\":{\"name\":\"CREATE_EVENT\",\"id\":142914,\"timestamp\":\"2016-08-02T18:47:23-0500\"}} "
				+ UUID.randomUUID().toString();

		AtomicReference<Event<KinesisRecord>> eventRef = new AtomicReference<Event<KinesisRecord>>(null);
		bus.on(Selectors.T(KinesisRecord.class), (Event<KinesisRecord> x) -> {

			try {

				String s = x.getData().getBodyAsString();

				log.info("Received: {}" + s);
				if (s.equals(message)) {
					latch.countDown();
					eventRef.set(x);
				}

			} catch (RuntimeException e) {
				e.printStackTrace();
			}
		});

		PutRecordResult xx = bridge.getKinesisClient().putRecord(getStreamName(), ByteBuffer.wrap(message.getBytes()),
				"workflow-runtime");

		boolean success = latch.await(2, TimeUnit.MINUTES);
		Assertions.assertThat(success).isTrue();

		Assertions.assertThat(eventRef.get()).isNotNull();
		Assertions.assertThat(eventRef.get().getKey()).isInstanceOf(KinesisRecord.class);

		Assertions.assertThat(eventRef.get().getKey()).isSameAs(eventRef.get().getData());
		// we don't do anything with headers

		KinesisRecord kr = eventRef.get().getData();
		Assertions.assertThat(kr.getStreamName()).isEqualTo(bridge.getStreamName());
		Assertions.assertThat(kr.getBridge()).isSameAs(bridge);
		Assertions.assertThat(bridge.getArn()).startsWith("arn:aws:kinesis:");
		Assertions.assertThat(kr.getStreamArn()).isEqualTo(bridge.getStreamArn());
		Assertions.assertThat(kr.getBodyAsString()).startsWith("{\"workItem");

		Record r = kr.getRecord();
		Assertions.assertThat(r.getSequenceNumber()).isNotNull();
		Assertions.assertThat(r.getPartitionKey()).isEqualTo("workflow-runtime");
		Assertions.assertThat(r.getApproximateArrivalTimestamp()).isCloseTo(new Date(), 120000);

		Thread.sleep(5000);

		bridge.stop();

		Thread.sleep(5000);
	}
}
