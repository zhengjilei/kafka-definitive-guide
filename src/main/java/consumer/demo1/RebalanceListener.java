package consumer.demo1;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class RebalanceListener implements ConsumerRebalanceListener {
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    private KafkaConsumer<String, String> consumer;

    public RebalanceListener(KafkaConsumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        this.consumer = consumer;
        this.currentOffsets = currentOffsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println("onPartitionsRevoked: " + currentOffsets);
        // 消费者停止读消息之后，再均衡开始之前，提交偏移量
        consumer.commitSync();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        // 重新分配分区之后，新的消费者开始读取消息之前
        System.out.println("onPartitionsAssigned: " + currentOffsets);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {

    }
}


