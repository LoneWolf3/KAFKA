package com.sac.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class UserConsumerThread implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final String topic;
	RebalanceListner rebalanceListner;

	public UserConsumerThread(String brokers, String groupId, String topic) {
		Properties prop = createConsumerConfig(brokers, groupId);
		this.topic = topic;
		this.consumer = new KafkaConsumer<>(prop);
		this.rebalanceListner = new RebalanceListner(consumer);
		this.consumer.subscribe(Arrays.asList(this.topic), rebalanceListner);
	}

	private static Properties createConsumerConfig(String brokers, String groupId) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	@Override
	public void run() {

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					// System.out.println("Topic:"+ record.topic() +" Partition:" +
					// record.partition() + " Offset:" + record.offset() + " Value:"+
					// record.value());
					// Do some processing and save it to Database
					rebalanceListner.addOffset(record.topic(), record.partition(), record.offset());
				}
				// consumer.commitSync(rebalanceListner.getCurrentOffsets());
			}
		} catch (Exception ex) {
			System.out.println("Exception.");
			ex.printStackTrace();
		} finally {
			consumer.close();
		}
	}

}
