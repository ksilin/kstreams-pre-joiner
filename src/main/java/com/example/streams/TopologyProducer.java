package com.example.streams;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.Optional;

/**
 * CDI Producer for the Kafka Streams Topology.
 * Quarkus will automatically manage the KafkaStreams lifecycle.
 */
@ApplicationScoped
public class TopologyProducer {

    @ConfigProperty(name = "app.topic.parent")
    String parentTopic;

    @ConfigProperty(name = "app.topic.child")
    String childTopic;

    @ConfigProperty(name = "app.topic.ready")
    String readyTopic;

    @ConfigProperty(name = "app.topic.error")
    String errorTopic;

    @ConfigProperty(name = "app.topic.parent-error")
    String parentErrorTopic;

    /// JSON pointer to the parent ID in the parent success record. Not set: the record key is the parent ID.
    @ConfigProperty(name = "app.parent-id.in-parent")
    Optional<String> parentIdPointer;

    /// JSON pointer to the parent ID in the child record. Not set: the record key is the parent ID.
    @ConfigProperty(name = "app.parent-id.in-child")
    Optional<String> childParentIdPointer;

    @ConfigProperty(name = "app.max-wait", defaultValue = "1h")
    Duration maxWait;

    @ConfigProperty(name = "app.expiry-check-interval", defaultValue = "10s")
    Duration expiryCheckInterval;

    @Produces
    public Topology buildTopology() {
        return PreJoinTopology.build(new PreJoinTopology.Settings(
                parentTopic, childTopic, readyTopic, errorTopic, parentErrorTopic,
                parentIdPointer, childParentIdPointer,
                maxWait, expiryCheckInterval));
    }
}
