package registry;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class SchemaProducer {
    public static void main(String[] args) throws IOException {
        String kafkaHost = "localhost:9092";
        String topic = "schema-tutorial";
        String schemaFilename = "/Users/ethanwalker/repo/java/kafka-definitive-guide/src/main/resources/user.json";
        String registryHost = "http://localhost:8081";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 指定Value的序列化类，KafkaAvroSerializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // 指定 registry 服务的地址
        // 如果 Schema Registry 启动了高可用，那么这儿的配置值可以是多个服务地址，以逗号隔开
        props.put("schema.registry.url", registryHost);
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        String key = "Alyssa key";
        Schema schema = new Schema.Parser().parse(new File(schemaFilename));
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", "Alyssa");
        avroRecord.put("favorite_number", 256);

        // 发送消息
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, key, avroRecord);
        producer.send(record);
        producer.flush();
        producer.close();

    }
}
