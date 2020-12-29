package com.prabal.test;

import java.util.Collections;
import java.util.stream.IntStream;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class CreateQueueMain {
	static Region region = Region.AP_SOUTH_1;
	static String QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/544169010123/my-queue";

	public static void main(String[] args) {
		SqsClient sqsClient = SqsClient.builder().region(region).build();
//		String queueUrl = createQueue(sqsClient, "my-queue");
		sendData(sqsClient);
	}

	public static String createQueue(SqsClient sqsClient, String queueName) {
		try {
			System.out.println("\nCreate Queue");
			CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName)
					.attributes(Collections.singletonMap(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "20"))
					.build();

			sqsClient.createQueue(createQueueRequest);
			System.out.println("\nGet queue url");

			GetQueueUrlResponse getQueueUrlResponse = sqsClient
					.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
			String queueUrl = getQueueUrlResponse.queueUrl();
			return queueUrl;

		} catch (SqsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		return "";
	}

	public static void sendData(SqsClient sqsClient) {
		Thread t = new Thread(() -> {
			IntStream.range(0, 30).sequential().forEach(i -> {
				System.out.println("sending message " + i);
				send(sqsClient, "Message count " + i);
			});
		});
		t.start();
	}

	public static void send(SqsClient sqsClient, String message) {
		try {
			sqsClient.sendMessage(
					SendMessageRequest.builder().queueUrl(QUEUE_URL).messageBody(message).delaySeconds(2).build());
		} catch (SqsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}
}
