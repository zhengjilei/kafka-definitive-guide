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
import java.util.concurrent.atomic.AtomicLong;

public class CommitOffsetManual2 {
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
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        consumer.subscribe(Collections.singletonList(topic), new RebalanceListener(consumer, currentOffsets));

        AtomicLong seq = new AtomicLong();
        long count = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));

                long maxOffset = 0;
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[Thread-1] topic: %s, partition: %s, offset: %s, key: %s, value: %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    Thread.sleep(1000);
                    // do something

                    count++;

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, null)); // 注意是 offset+1, 表示期望处理的下一条消息的偏移量
                    if (count % 1000 == 0) { // 每收到100条记录手动提交一次
                        consumer.commitAsync(currentOffsets, null);
                    }
                }

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
