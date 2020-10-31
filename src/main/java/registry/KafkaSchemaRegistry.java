package registry;

import org.apache.avro.Schema;
//
//public class KafkaSchemaRegistry implements SchemaRegistry, MasterAwareSchemaRegistry {
//
//    public int registerOrForward(String subject,
//                                 Schema schema,
//                                 Map<String, String> headerProperties)
//            throws SchemaRegistryException {
//        // 检测这个schema是否之前注册过
//        Schema existingSchema = lookUpSchemaUnderSubject(subject, schema, false);
//        if (existingSchema != null) {
//            if (schema.getId() != null && schema.getId() >= 0 && !schema.getId().equals(existingSchema.getId())
//            ) {
//                throw new IdDoesNotMatchException(existingSchema.getId(), schema.getId());
//            }
//            return existingSchema.getId();
//        }
//
//        synchronized (masterLock) {
//            if (isMaster()) {
//                // 如果是leader，那么执行register方法，写schema到kafka
//                return register(subject, schema);
//            } else {
//                // 如果是follower，那么转发请求到 leader
//                if (masterIdentity != null) {
//                    return forwardRegisterRequestToMaster(subject, schema, headerProperties);
//                } else {
//                    throw new UnknownMasterException("Register schema request failed since master is "
//                            + "unknown");
//                }
//            }
//        }
//    }
//}
//
