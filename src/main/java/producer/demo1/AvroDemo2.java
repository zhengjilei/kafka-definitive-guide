package producer.demo1;

import examples.payment.Payment;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Properties;

public class AvroDemo2 {
    public static void main(String[] args) {
        String topic = "customerContacts-avro";

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProps);

        Payment p = Payment.newBuilder().setId("ID-abc").setAmount(10).setFavoriteColor(null).build();
        GenericRecord avroRecord = new GenericData.Record(p.getSchema());
        avroRecord.put("id", "it.a");
        avroRecord.put("amount", 12342.324); // 未定义成 type: ["null"] 的类型，必须设置值
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, "b-key", avroRecord);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
