package com.lahoti.kafka.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaProducerDemoWithCallBack {
	
	private static Logger logger = LoggerFactory.getLogger(KafkaProducerDemoWithCallBack.class.getName()); 
	
	public static void main(String args[]) throws InterruptedException, ExecutionException{
		//create producer properties
		String bootstrapServers = "127.0.0.1:9092";
		String topic = "candidate-update";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Safe Producer Config
//		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//		properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
//		properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
//		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
		
		//High Throughput producer
//		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
//		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
//		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
//		
		//create the producer
		KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
		
		
		//create a producer record
		Map<String,String> kafkaMessage = new HashMap<>();
		kafkaMessage.put("id_1", "{\"event\":\"CREATE\",\"recordId\":1,\"name\":\"Joe Programmer\"}");
		kafkaMessage.put("id_2", "{\"event\":\"UPDATE\",\"recordId\":1,\"name\":\"Java Programmer\"}");
		kafkaMessage.put("id_3", "{\"event\":\"DELETE\",\"recordId\":1}");
		
		for (Map.Entry<String,String> entry : kafkaMessage.entrySet()){
			String key = entry.getKey();
			String value = entry.getValue();
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,key,value);
			
			//send data - asynchronous
			producer.send(producerRecord,new Callback() {
	
				@Override
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					if(e == null){
						logger.info("Received new metadata. \n" +
									"Topic : " + recordMetadata.topic() + "\n" +
									"Partition : " + recordMetadata.partition() + "\n" +
									"Offset : " + recordMetadata.offset() + "\n" +
									"Timestamp : " + recordMetadata.timestamp()
								);
					}else{
						logger.error("Error while producing " + e);
					}
					
				}
				
			});
		}
		
		//flush and close producer
		producer.close();
		
		
	}
	
	

}
