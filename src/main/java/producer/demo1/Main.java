package producer.demo1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);

        for (int i = 0; i < 30; i++) {
            Thread.sleep(300);
            ProducerRecord<String, String> record = new ProducerRecord<>("custom-country", Integer.toString(i), "country_" + i);
            try {
                RecordMetadata data = (RecordMetadata) producer.send(record, new ProducerCallback()).get();
                System.out.println("data = " + data);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}


