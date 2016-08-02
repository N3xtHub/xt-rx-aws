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
package com.xtrac.reactor.aws.sns;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.policy.conditions.ConditionFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSAsyncClient;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.google.common.collect.ImmutableList;
import com.xtrac.Config;
import com.xtrac.reactor.aws.sns.SNSMessage;
import com.xtrac.reactor.aws.sns.SNSSelectors;
import com.xtrac.reactor.aws.sqs.AbstractSQSIntegrationTest;
import com.xtrac.reactor.aws.sqs.SQSReactorBridge;

import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selectors;

public class SNSTest {
	
	static Logger log = LoggerFactory.getLogger(SNSTest.class);
	
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
	
	public static final void main(String [] args) throws InterruptedException  {
		
		EventBus bus = EventBus.create(Environment.initializeIfEmpty());
		String message = "{\n" + 
				"  \"Type\" : \"Notification\",\n" + 
				"  \"MessageId\" : \"abc2b453-349a-5637-b17c-fbc4aa639b6c\",\n" + 
				"  \"TopicArn\" : \"arn:aws:sns:us-east-1:550588888888:test\",\n" + 
				"  \"Subject\" : \"world\",\n" + 
				"  \"Message\" : \"hello\",\n" + 
				"  \"Timestamp\" : \"2016-04-25T04:27:37.504Z\",\n" + 
				"  \"SignatureVersion\" : \"1\",\n" + 
				"  \"Signature\" : \"SIGDATASIGDATA3Jee8fReP4hdnirjTyRy6TDk7lewTApqLnff882FCQMeDjr8XF3q4oHRcDCOYyy2eOHOafBJnSCPs0DgSJ3A3cNl9OeLeq4INg==\",\n" + 
				"  \"SigningCertURL\" : \"https://sns.us-east-1.amazonaws.com/SimpleNotificationService-bb750dd426d95ee93901aaaaaaaaaaaa.pem\",\n" + 
				"  \"UnsubscribeURL\" : \"https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:550534291128:test:d969ed7a-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"\n" + 
				"}";
		DefaultAWSCredentialsProviderChain chain = new DefaultAWSCredentialsProviderChain();
		
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
	        
	   
	        
	     
		Regions region = Regions.fromName(config.getRegionName() );    	 
		 
		 
		AmazonSNSAsyncClient sns = new AmazonSNSAsyncClient(clientConfiguration);
		sns.setRegion(Region.getRegion( region));
		CreateTopicRequest createTopicRequest = new CreateTopicRequest("test");
		CreateTopicResult createResult = sns.createTopic(createTopicRequest);
		String snsArn = createResult.getTopicArn();
		
		AmazonSQSAsyncClient client = new AmazonSQSAsyncClient(clientConfiguration);
		client.setRegion(Region.getRegion( region));
		CreateQueueRequest request = new CreateQueueRequest("test");
		CreateQueueResult result = client.createQueue(request);
		String sqsName = result.getQueueUrl();
		

		GetQueueAttributesRequest queueAttributesRequest = new GetQueueAttributesRequest(sqsName)
		 .withAttributeNames("All");
		GetQueueAttributesResult queueAttributesResult = client.getQueueAttributes(queueAttributesRequest);
		Map<String, String> sqsAttributeMap = queueAttributesResult.getAttributes();
		String sqsArn = sqsAttributeMap.get("QueueArn"); 

		SubscribeRequest subscribeRequest = new SubscribeRequest(snsArn, "sqs", sqsArn);
		SubscribeResult subscribeResult = sns.subscribe(subscribeRequest);
		String subscriptionArn = subscribeResult.getSubscriptionArn();
		
		Statement statement = new Statement(Effect.Allow)
				 .withActions(SQSActions.SendMessage)
				 .withPrincipals(new Principal("*"))
				 .withConditions(ConditionFactory.newSourceArnCondition(snsArn))
				 .withResources(new Resource(sqsArn));
				Policy policy = new Policy("SubscriptionPermission")
				 .withStatements(statement);
		
		HashMap<String, String> attributes = new HashMap<String, String>();
		attributes.put("Policy", policy.toJson());
		SetQueueAttributesRequest req = new SetQueueAttributesRequest(sqsName, attributes);
		client.setQueueAttributes(req);


		SQSReactorBridge bridge = new SQSReactorBridge.Builder()
				.withSNSSupport(true)
				.withEventBus(bus)
				.withSQSClient(client)
				.withUrl(sqsName)
				.withRegion(config.getRegionName())
				.withClientConfiguration(clientConfiguration)
				.build()
				.start();
		 
		
		bus.on(SNSSelectors.snsTopicSelector("test"), (Event<SNSMessage> c)-> {
			log.info(">> "+c.getData().getSubject());
			log.info(c.getData().getTopicArn());
			log.info(c.getData().getBodyAsJson().toString());
		});
		
		
		sns.publish("arn:aws:sns:us-east-1:132368745812:test", "{\"workItem\":{\"fileNumber\":\"W026938-02AUG16\",\"orgId\":1021,\"itemType\":\"Project Request\",\"status\":\"APRQ\",\"statusType\":\"P\",\"statusDate\":\"2016-08-02T18:47:23-0500\",\"suspenseStatus\":\"A\",\"splitStatus\":\"N\",\"memo\":\" \",\"amount\":0,\"currentQueue\":\"A045103\",\"currentNode\":\"Management\",\"nodeAccessGroup\":\"Management\",\"creatingOperator\":\"A045103\",\"nodeWhereCreated\":\"Management\",\"createDate\":\"2016-08-02T18:47:23-0500\",\"lastEvent\":\"ENTER\",\"lastEventDate\":\"2016-08-02T18:47:23-0500\",\"priorityNumber\":0,\"suspensionCount\":0,\"suspensionDuration\":0,\"queueType\":1,\"queueEnterTime\":\"2016-08-02T18:47:23-0500\",\"parentFileNumber\":0,\"nonCntrSuspensionCount\":0,\"nonCntrSuspensionDuration\":0,\"familyId\":26938080216,\"modOperId\":\"A045103\",\"noteCount\":0,\"parties\":[{\"partyNumber\":1,\"name\":\"ORIG\",\"taxReportingCode\":\" \",\"organizationName\":\"ACME\",\"accountNumber\":\"111\"}]},\"eventDetail\":{\"name\":\"CREATE_EVENT\",\"id\":142914,\"timestamp\":\"2016-08-02T18:47:23-0500\"}}", "XT-Event");
		//log.info(topicArn);
		
		
	
		Thread.sleep(30000L);
	}
}
