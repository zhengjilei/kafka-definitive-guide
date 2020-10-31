package registry;
//
//import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
//import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
//import org.apache.avro.Schema;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//
//public class CachedSchemaRegistryClient implements SchemaRegistryClient {
//    // Key 为数据格式的名称， 里面的 Value 为 Map类型，它对于的 Key 为数据格式，Value 为对应的 id 号
//    private final Map<String, Map<Schema, Integer>> schemaCache;
//    // Key 为数据格式的名称，里面的 Value 为 Map类型，它对于的 Key 为 id 号，Value 为对应的数据格式
//    // 这个集合比较特殊，当 Key 为 null 时，表示 id 到 数据格式的缓存
//    private final Map<String, Map<Integer, Schema>> idCache;
//
//    public CachedSchemaRegistryClient(Map<String, Map<Schema, Integer>> schemaCache, Map<String, Map<Integer, Schema>> idCache) {
//        this.schemaCache = schemaCache;
//        this.idCache = idCache;
//    }
//
//
//    @Override
//    public synchronized int register(String subject, Schema schema, int version, int id)
//            throws IOException, RestClientException {
//        // 从schemaCache查找缓存，如果不存在则初始化空的哈希表
//        final Map<Schema, Integer> schemaIdMap =
//                schemaCache.computeIfAbsent(subject, k -> new HashMap<>());
//
//        // 获取对应的 id 号
//        final Integer cachedId = schemaIdMap.get(schema);
//        if (cachedId != null) {
//            // 检查 id 号是否有冲突
//            if (id >= 0 && id != cachedId) {
//                throw new IllegalStateException("Schema already registered with id "
//                        + cachedId + " instead of input id " + id);
//            }
//            // 返回缓存的 id 号
//            return cachedId;
//        }
//
//        if (schemaIdMap.size() >= identityMapCapacity) {
//            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
//        }
//
//        // 如果缓存没有，则向服务端发送 http 请求
//        final int retrievedId = id >= 0
//                ? registerAndGetId(subject, schema, version, id)
//                : registerAndGetId(subject, schema);
//        // 缓存结果
//        schemaIdMap.put(schema, retrievedId);
//        idCache.get(null).put(retrievedId, schema);
//        return retrievedId;
//    }
//}
