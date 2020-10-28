package producer.demo1;

import io.confluent.common.utils.Utils;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class BananaPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if ((bytes == null) || (!(key instanceof String))) {
            throw new InvalidRecordException("we expect all messages to have customer name as key");
        }
        if (((String) key).equals("Banana")) {
            return numPartitions;// Banana 总是分配到最后一个分区
        }
        return (Math.abs(Utils.murmur2(bytes)) % (numPartitions - 1));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
