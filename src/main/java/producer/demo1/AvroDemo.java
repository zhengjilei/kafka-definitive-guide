package producer.demo1;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AvroDemo {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        String topic = "customerContacts";
        // 非 Arvo 生成的对象，需要手动提供 Schema
        String schemaString = "{\"namespace\": \"customerManagement.avro\", \"type\": \"record\", " +
                "\"name\": \"Customer\"," +
                "\"fields\": [" +
                "{\"name\": \"id\", \"type\": \"int\"}," +
                "{\"name\": \"name\", \"type\": \"string\"}," +
                "{\"name\": \"email\", \"type\": [\"null\",\"string\"], \"default\":null }" +
                "]}";
        Producer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProps);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);
        try {
            for (int i = 0; i < 10; i++) {
                GenericRecord customer = new GenericData.Record(schema);
                String name = "ethan_" + i;
                customer.put("id", i);
                customer.put("name", name);
                customer.put("email", name + "@gmail.com");

                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, name, customer);
                producer.send(record);

                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("main over");
    }
}
