package com.prabal.test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class TestMain {
	static Region region = Region.AP_SOUTH_1;
	static String QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/544169010123/my-queue";
	static AtomicBoolean state = new AtomicBoolean(true);
	static ReentrantLock lock = new ReentrantLock();
	static AtomicLong count = new AtomicLong(0);

	public static void main(String[] args) throws InterruptedException {
		SqsClient sqsClient = SqsClient.builder().region(region).build();
//		String queueUrl = createQueue(sqsClient, "my-queue");
//		listQueues(sqsClient);
//		sendData(sqsClient);
		receiveMessage();
		synchronized (lock) {
			lock.wait();
		}
	}

	public static void receiveMessage() {
		System.out.println("receeing ");
		SqsAsyncClient sqsClient = SqsAsyncClient.builder().region(region).build();
		ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(QUEUE_URL).maxNumberOfMessages(5)
				.waitTimeSeconds(10).build();

//		Thread th = new Thread(() -> {
			Flux<ReceiveMessageResponse> flux = Flux.push(sink -> {
				sink.onRequest(consumer -> {
					System.out.println("Thread on Source: " + Thread.currentThread().getName());
					sqsClient.receiveMessage(request).thenAccept(m -> {
						System.out.println("received messages size " + m.messages().size());
						sink.next(m);
					});
				});
			});
			flux.subscribeOn(Schedulers.boundedElastic()).flatMap(r -> {
				return Flux.fromIterable(r.messages());
			}).subscribe(new CustomSubscription(sqsClient, QUEUE_URL));
//		});
//		th.start();

//		sqsClient.receiveMessage(request).thenAccept(m -> {
//			System.out.println("received messages size " + m.messages().size());
//			m.messages().forEach(System.out::println);
//		});
	}

	public static void listQueues(SqsClient sqsClient) {
		sqsClient.listQueues().queueUrls().forEach(System.out::println);
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
			IntStream.range(0, 10).sequential().forEach(i -> {
				if (state.get()) {
					try {
						System.out.println("sending message " + i);
						send(sqsClient, "Message count " + i);
						Thread.currentThread().sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					System.out.println("closing");
				}
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
