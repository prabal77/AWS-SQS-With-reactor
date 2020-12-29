package com.prabal.test;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class Test2 {
	static String QUEUE_NAME = "https://sqs.ap-south-1.amazonaws.com/544169010123/my-queue";
	static Region region = Region.AP_SOUTH_1;
	static Object o = new Object();

	public static void main(String[] args) throws InterruptedException {
		SqsAsyncClient sqsClient = SqsAsyncClient.builder().region(region).build();
		ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(QUEUE_NAME).maxNumberOfMessages(5)
				.waitTimeSeconds(10).build();

		Flux<ReceiveMessageResponse> flux = Flux.push(sink -> {
			sink.onRequest(consumer -> {
				System.out.println("Thread on Source: " + Thread.currentThread().getName());
				sqsClient.receiveMessage(request).thenAccept(m -> {
					System.out.println("received messages size " + m.messages().size());
					sink.next(m);
				});
			});
		});
		flux.parallel().log()
//		.subscribeOn(Schedulers.boundedElastic())
		.flatMap(r -> {
			return Flux.fromIterable(r.messages());
		}).subscribe(new CustomSubscription(sqsClient, QUEUE_NAME));
		
		synchronized (o) {
			o.wait();
		}
	}

}
