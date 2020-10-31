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
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.util.Properties;

public class AvroProducer2 {
    public static void main(String[] args) throws InterruptedException {
        String topic = "payment-avro";

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProps);

        Payment p = Payment.newBuilder().setId("ID-abc").setAmount(10).setFavoriteColor(null).build();
        for (int i = 0; i < 10; i++) {
            GenericRecord avroRecord = new GenericData.Record(p.getSchema());
            avroRecord.put("id", "it_" + i);
            avroRecord.put("amount", 12342.324); // 未定义成 type: ["null"] 的类型，必须设置值
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, "b-key", avroRecord);
            producer.send(record);
            Thread.sleep(2000);
        }
        producer.flush();
        producer.close();
    }
}
