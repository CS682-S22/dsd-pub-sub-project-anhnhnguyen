package project2.zookeeper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;

import java.io.ByteArrayOutputStream;

/**
 * Class to serialize and deserialize a Jackson instance.
 *
 * @author anhnguyen
 * <p>
 * Reference: http://blog.palominolabs.com/2012/08/14/using-netflix-curator-for-service-discovery/index.html
 */
public class JacksonInstanceSerializer<T> implements InstanceSerializer<T> {
    /**
     * type reference.
     */
    private final TypeReference<ServiceInstance<T>> typeReference;
    /**
     * object reader.
     */
    private final ObjectReader objectReader;
    /**
     * object writer.
     */
    private final ObjectWriter objectWriter;

    /**
     * Constructor.
     *
     * @param objectReader  object reader
     * @param objectWriter  object writer
     * @param typeReference type reference
     */
    public JacksonInstanceSerializer(ObjectReader objectReader, ObjectWriter objectWriter,
                                     TypeReference<ServiceInstance<T>> typeReference) {
        this.typeReference = typeReference;
        this.objectReader = objectReader;
        this.objectWriter = objectWriter;
    }

    /**
     * Serialize the service instance.
     *
     * @param serviceInstance service instance
     * @return byte array
     * @throws Exception exception
     */
    @Override
    public byte[] serialize(ServiceInstance serviceInstance) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        objectWriter.writeValue(baos, serviceInstance);
        return baos.toByteArray();
    }

    /**
     * Deserialize the service instance.
     *
     * @param bytes byte array
     * @return Service Instance
     * @throws Exception exception
     */
    @Override
    public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
        return objectReader.forType(typeReference).readValue(bytes);
    }
}
