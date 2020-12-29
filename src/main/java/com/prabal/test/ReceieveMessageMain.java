package com.prabal.test;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class ReceieveMessageMain {
	static Region region = Region.AP_SOUTH_1;
	static String QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/544169010123/my-queue";
	static Object o = new Object();
	static Logger log = LoggerFactory.getLogger(ReceiveMessageRequest.class);

	public static void main(String[] args) throws InterruptedException {
		SqsAsyncClient sqsClient = SqsAsyncClient.builder().region(region).build();
		ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(QUEUE_URL).maxNumberOfMessages(5)
				.waitTimeSeconds(10).build();

		sqsClient.receiveMessage(request).thenApply(res -> {
			if (res.hasMessages()) {
				// Add the received messages to the internal store for housekeeping
				addReceivedMessagesToStore(res, LocalDateTime.now());
			}
			return res.messages();
		});

		synchronized (o) {
			o.wait();
		}
	}

	public static void addReceivedMessagesToStore(ReceiveMessageResponse response, LocalDateTime messageReceivedTime) {
		MessageLifeCycleManager.manageMessages(response.messages(), messageReceivedTime);
	}

}
