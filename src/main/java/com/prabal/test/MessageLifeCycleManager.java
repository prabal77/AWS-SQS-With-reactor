package com.prabal.test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.sqs.model.Message;

// singleton
public class MessageLifeCycleManager {
	ConcurrentHashMap<String, Message> messageStore = new ConcurrentHashMap<>();
	DelayQueue<MessageWithReceivedTime> queue = new DelayQueue<MessageWithReceivedTime>();
	// Fixed for a queue
	static final long visibilityTimeout = 30l;
	static final long DEFAULT_BUFFER_TIME = 5l; // 5 second

	static final MessageLifeCycleManager INSTANCE = new MessageLifeCycleManager();
	// Thread to check the delay queue
	Thread lifeCycleManager;

	public MessageLifeCycleManager() {
		start();
	}

	private void start() {
		this.lifeCycleManager = new Thread(() -> {
			while (true) {
				try {
					System.out.println(
							"Currently inflight " + this.messageStore.size() + " in delay queue " + this.queue.size());
					MessageWithReceivedTime m = queue.poll(15, TimeUnit.SECONDS);
					if (m != null)
						System.out.println("-----------> " + m.toString());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		this.lifeCycleManager.setDaemon(true);
		this.lifeCycleManager.start();
	}

	public static void manageMessages(List<Message> messages, LocalDateTime receivedTime) {
		messages.forEach(message -> {
			System.out.println("Messages added "+message.messageId());
			INSTANCE.messageStore.put(message.messageId(), message);
			INSTANCE.queue.add(new MessageWithReceivedTime(message, receivedTime));
		});
	}

	/**
	 * Wraper object to handle delay queue
	 * 
	 * @author prabal
	 *
	 */
	public static class MessageWithReceivedTime implements Delayed {
		private final Message originalMessage;
		private final LocalDateTime expirationTime;

		public MessageWithReceivedTime(Message originalMessage, LocalDateTime receivedTime) {
			this.originalMessage = originalMessage;
			this.expirationTime = receivedTime.plus((visibilityTimeout - DEFAULT_BUFFER_TIME), ChronoUnit.SECONDS);
		}

		public String getMessageId() {
			return this.originalMessage.messageId();
		}

		public LocalDateTime getExpirationTime() {
			return this.expirationTime;
		}

		public int compareTo(Delayed other) {
			MessageWithReceivedTime otherObject = (MessageWithReceivedTime) other;
			if (this.getMessageId().equals(otherObject.getMessageId())) {
				return 0;
			} else if (this.getExpirationTime().isBefore(otherObject.getExpirationTime())) {
				return -1;
			} else {
				return 1;
			}
		}

		@Override
		public long getDelay(TimeUnit timeunit) {
			return LocalDateTime.now().until(this.expirationTime, timeunit.toChronoUnit());
		}

		@Override
		public String toString() {
			return "MessageWithReceivedTime [originalMessage=" + originalMessage + ", expirationTime=" + expirationTime
					+ "]";
		}
	}
}
