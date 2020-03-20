package com.lahoti.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDemo {
	
	private static Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class.getName()); 
	
	public static void main(String args[]) throws InterruptedException, ExecutionException{
		//create consumer properties
		String bootstrapServers = "127.0.0.1:9092";
		String group_id = "my-fourth-application";
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		
		//create the consumer
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
		
			String topic = "transaction-log";
			
			//subscribe the consumer to the topic
			consumer.subscribe(Arrays.asList(topic));
			
			//poll for new data
			while(true){
				ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				
				for(ConsumerRecord<String,String> consumerRecord : consumerRecords){
					logger.info("Key : " + consumerRecord.key() + ", Value : " + consumerRecord.value());
					logger.info("Partition : " + consumerRecord.partition() + ", Offset : " + consumerRecord.offset());
				}
			}		
		
	}
	
	

}
