package com.prabal.test;

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;

public class CustomSubscription extends BaseSubscriber<Message> {
	private SqsAsyncClient client;
	private String QUEUE_URL;
	private AtomicLong count = new AtomicLong(0);

	public CustomSubscription(SqsAsyncClient client, String url) {
		this.client = client;
		this.QUEUE_URL = url;
	}

	@Override
	protected void hookOnNext(Message value) {
		System.out.println("---> " + value.body());
		client.deleteMessage(
				DeleteMessageRequest.builder().queueUrl(QUEUE_URL).receiptHandle(value.receiptHandle()).build());
		if (count.incrementAndGet() < 10) {
			System.out.println("requesting");
			request(100);
		}
	}

	@Override
	protected void hookOnComplete() {
		System.out.println("completed");
	}

	@Override
	protected void hookOnError(Throwable throwable) {
		System.out.println("Error happend " + throwable.getMessage());
	}

	@Override
	protected void hookOnSubscribe(Subscription subscription) {
		System.out.println("requesting on subscription");
		request(1);
	}
}
