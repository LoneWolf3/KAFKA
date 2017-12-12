package com.sac.main;

import com.sac.consumer.UserConsumerThread;
import com.sac.producer.UserProducerThread;

public final class Main {

	public static void main(String[] args) {

		String brokers = "10.121.250.186:9092";
		String groupId = "group01";
		String topic = "UserMessageTopic";

		if (args != null && args.length == 3) {
			brokers = args[0];
			groupId = args[1];
			topic = args[2];
		}

		// Start User Producer

		UserProducerThread producerThread = new UserProducerThread(brokers, topic);
		Thread t1 = new Thread(producerThread);
		t1.start();

		// Start group of User Consumer Thread

		/*
		 * UserConsumerThread consumerThread = new UserConsumerThread(brokers, groupId,
		 * topic); Thread t2 = new Thread(consumerThread); t2.start();
		 */

		try {
			Thread.sleep(100000);
		} catch (InterruptedException ie) {

		}
	}
}