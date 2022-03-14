package project2.zookeeper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;

/**
 * Instance serializer factory to pass the Host object to the Service Discovery.
 *
 * @author anhnguyen
 * <p>
 * Reference: http://blog.palominolabs.com/2012/08/14/using-netflix-curator-for-service-discovery/index.html
 */
public class InstanceSerializerFactory {
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
     * @param objectReader object reader
     * @param objectWriter object writer
     */
    public InstanceSerializerFactory(ObjectReader objectReader, ObjectWriter objectWriter) {
        this.objectReader = objectReader;
        this.objectWriter = objectWriter;
    }

    /**
     * Getter for an instance serializer.
     *
     * @param typeReference type reference
     * @return JacksonInstanceSerializer
     */
    public <T> InstanceSerializer<T> getInstanceSerializer(TypeReference<ServiceInstance<T>> typeReference) {
        return new JacksonInstanceSerializer<>(objectReader, objectWriter, typeReference);
    }
}
