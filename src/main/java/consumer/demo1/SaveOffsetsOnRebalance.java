package consumer.demo1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
    private KafkaConsumer<String, String> consumer;

    private static Map<String, Long> DBOffsetsStore = new HashMap<>();//  key = topic_name:partition_name

    public SaveOffsetsOnRebalance(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        commitDBTransaction();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        //
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, getOffsetFromDB(partition));
        }
    }

    static void commitDBTransaction() {
        return;
    }

    static long getOffsetFromDB(TopicPartition partition) {
        return DBOffsetsStore.get(partition.topic() + ":" + partition.partition());
    }

    static void setOffsetToDB(ConsumerRecord<String, String> record) {
        DBOffsetsStore.put(record.topic() + ":" + record.partition(), record.offset() + 1);
    }

    public static void main(String[] args) {


        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        prop.put("group.id", "CountryCounter"); // 消费者群组
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        String topic = "custom-country";

        consumer.subscribe(Collections.singletonList(topic), new SaveOffsetsOnRebalance(consumer));
        consumer.poll(0); // 让消费者加入到消费者群组，并获取到分配的分区，立即返回，不等待消息返回

        for (TopicPartition partition : consumer.assignment()) {
            consumer.seek(partition, getOffsetFromDB(partition));
        }

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                System.out.println("System exit");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        while (true) {
            // start transaction
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // do something to process record

                setOffsetToDB(record);
            }
            commitDBTransaction();
        }
    }
}




