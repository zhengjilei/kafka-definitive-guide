package consumer.demo1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CommitOffsetManual {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000); // 自动 commit offset 的间隔是 10s

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

        AtomicLong seq = new AtomicLong();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));

                long maxOffset = 0;
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[Thread-1] topic: %s, partition: %s, offset: %s, key: %s, value: %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    Thread.sleep(1000);
                    // do something

                    if (record.offset() > maxOffset) {
                        maxOffset = record.offset();
                    }

                    synchronized (lock) {
                        if (!countryUpdatedMap.containsKey(record.value())) {
                            countryUpdatedMap.put(record.value(), 1);
                        } else {
                            countryUpdatedMap.put(record.value(), countryUpdatedMap.get(record.value()) + 1);
                        }
                    }
                }

                long finalMaxOffset = maxOffset;
                consumer.commitAsync((map, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                        if (seq.get() == finalMaxOffset) {
                            consumer.commitAsync(); // 重试
                        } else {
                            // 表明已经有一个新的偏移量更大的提交已经发送出去了，不需要再重试
                        }
                        return;
                    }
                    // success
                    seq.set(finalMaxOffset);
                });
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                consumer.commitSync(); // commitSync 方法会一直重试，直至提交成功或者发生无法恢复的错误
            } finally {
                consumer.close(); // 如果 autocommit=true, 执行close 也会自动提交
            }
        }

    }
}
