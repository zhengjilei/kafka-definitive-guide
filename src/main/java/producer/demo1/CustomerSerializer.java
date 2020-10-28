package producer.demo1;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public byte[] serialize(String s, Customer customer) {
        byte[] serializedName;
        int nameSize;
        try {

            if (customer == null) {
                return null;
            }
            if (customer.getName() != null) {
                serializedName = customer.getName().getBytes("UTF-8");
                nameSize = serializedName.length;
            } else {
                serializedName = new byte[0];
                nameSize = 0;
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + nameSize);
            buffer.putInt(customer.getId());
            buffer.putInt(nameSize);
            buffer.put(serializedName);

            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no action
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Customer data) {
        return new byte[0];
    }

    @Override
    public void close() {
        // no action
    }
}
