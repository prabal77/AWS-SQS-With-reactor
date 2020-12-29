package com.prabal.test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class TestMain {
	static Region region = Region.AP_SOUTH_1;
	static String QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/544169010123/my-queue";
	static AtomicBoolean state = new AtomicBoolean(true);
	static ReentrantLock lock = new ReentrantLock();
	static AtomicLong count = new AtomicLong(0);

	public static void main(String[] args) throws InterruptedException {
		SqsClient sqsClient = SqsClient.builder().region(region).build();
		listQueues(sqsClient);
//		receiveMessage();
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

}
