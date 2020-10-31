package consumer.demo2;

import examples.payment.Payment;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class AvroConsumer {

    private KafkaConsumer<String, Payment> consumer;
    private Properties properties = new Properties();

    public static void main(String[] args) {
        AvroConsumer avroConsumer = new AvroConsumer();
        avroConsumer.properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        avroConsumer.properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        avroConsumer.properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        avroConsumer.properties.put("schema.registry.url", "http://localhost:8081");
        avroConsumer.properties.put("group.id", "paymentConsumer"); // 消费者群组
        avroConsumer.properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); // 必须设置 specific.avro.reader = true

        String topic = "payment-avro";

        avroConsumer.consumer = new KafkaConsumer(avroConsumer.properties);
        avroConsumer.consumer.subscribe(Collections.singleton(topic));
        System.out.println("Reading topic: " + topic);

        try {
            while (true) {
                ConsumerRecords<String, Payment> records = avroConsumer.consumer.poll(1000);
                for (ConsumerRecord<String, Payment> record : records) {
                    System.out.println("payment: " + record.value().getId());
                }
                avroConsumer.consumer.commitAsync();
            }
        } finally {
            avroConsumer.consumer.close();
        }
    }

}
