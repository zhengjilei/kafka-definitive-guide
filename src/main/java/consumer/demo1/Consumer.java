package consumer.demo1;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Consumer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        prop.put("group.id", "CountryCounter"); // 消费者群组
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        String topic = "custom-country";

        consumer.subscribe(Collections.singletonList(topic));
        Map<String, Integer> countryUpdatedMap = new HashMap<>();


        Object lock = new Object();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (lock) {
                        System.out.printf("[Thead-2] countryUpdatedMap: %s\n", countryUpdatedMap);
                    }
                }

            }
        });
        thread.start();

        try {
            // 轮询
            while (true) {

                // 如果 autocommit=true, 每次poll 会将上一次的返回的最新偏移量提交
                // 心跳(避免会话过期) + 触发分区再平衡(如果有消费者失连) + 拉取消息(消息可能来自多个分区)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //
//                System.out.println("Thread-1] --------  poll over --------");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[Thread-1] topic: %s, partition: %s, offset: %s, key: %s, value: %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    synchronized (lock) {
                        if (!countryUpdatedMap.containsKey(record.value())) {
                            countryUpdatedMap.put(record.value(), 1);
                        } else {
                            countryUpdatedMap.put(record.value(), countryUpdatedMap.get(record.value()) + 1);
                        }
                    }
                }
            }
        } finally {
            System.out.println("hhhh closed");
            consumer.close();
        }

    }
}
