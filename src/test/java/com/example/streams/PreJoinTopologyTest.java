package com.example.streams;

import com.example.processor.PreJoinProcessor;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@DisplayName("PreJoinTopology Tests")
class PreJoinTopologyTest {

    private static final String PARENT_TOPIC = "financial-account-success";
    private static final String CHILD_TOPIC = "balances";
    private static final String READY_TOPIC = "balances-ready";
    private static final String ERROR_TOPIC = "balances-errors";
    private static final String PARENT_ERROR_TOPIC = "financial-account-success-errors";

    private static final Duration MAX_WAIT = Duration.ofMinutes(60);
    private static final Duration EXPIRY_CHECK_INTERVAL = Duration.ofSeconds(10);

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> parentTopic;
    private TestInputTopic<String, String> childTopic;
    private TestOutputTopic<String, String> readyTopic;
    private TestOutputTopic<String, String> errorTopic;
    private TestOutputTopic<String, String> parentErrorTopic;

    @BeforeEach
    void setUp() {
        Topology topology = PreJoinTopology.build(new PreJoinTopology.Settings(
                PARENT_TOPIC, CHILD_TOPIC, READY_TOPIC, ERROR_TOPIC, PARENT_ERROR_TOPIC,
                Optional.of("/externalId"), Optional.of("/financialAccountId"),
                MAX_WAIT, EXPIRY_CHECK_INTERVAL));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pre-joiner-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        testDriver = new TopologyTestDriver(topology, props);

        parentTopic = testDriver.createInputTopic(PARENT_TOPIC, new StringSerializer(), new StringSerializer());
        childTopic = testDriver.createInputTopic(CHILD_TOPIC, new StringSerializer(), new StringSerializer());
        readyTopic = testDriver.createOutputTopic(READY_TOPIC, new StringDeserializer(), new StringDeserializer());
        errorTopic = testDriver.createOutputTopic(ERROR_TOPIC, new StringDeserializer(), new StringDeserializer());
        parentErrorTopic = testDriver.createOutputTopic(PARENT_ERROR_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("Child before parent: the child waits, then the parent releases it")
    void childWaitsForParent() {
        childTopic.pipeInput("BAL-1", balance("BAL-1", "FA-1", 100));

        assertThat(readyTopic.isEmpty()).isTrue();

        parentTopic.pipeInput("FA-1", account("FA-1"));

        assertThat(readyTopic.readKeyValuesToList())
                .containsExactly(KeyValue.pair("BAL-1", balance("BAL-1", "FA-1", 100)));
        assertThat(errorTopic.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Parent before child: the child goes through at once")
    void parentKnownChildPassesThrough() {
        parentTopic.pipeInput("FA-1", account("FA-1"));
        childTopic.pipeInput("BAL-1", balance("BAL-1", "FA-1", 100));

        assertThat(readyTopic.readKeyValuesToList())
                .containsExactly(KeyValue.pair("BAL-1", balance("BAL-1", "FA-1", 100)));
    }

    @Test
    @DisplayName("Waiting children are released in arrival order, only for their own parent")
    void releaseKeepsOrderAndScope() {
        childTopic.pipeInput("BAL-1", balance("BAL-1", "FA-1", 100));
        childTopic.pipeInput("BAL-9", balance("BAL-9", "FA-10", 999));
        childTopic.pipeInput("BAL-1", balance("BAL-1", "FA-1", 110));
        childTopic.pipeInput("BAL-2", balance("BAL-2", "FA-1", 200));

        parentTopic.pipeInput("FA-1", account("FA-1"));

        // FA-10 starts with "FA-1", but its child must not be released
        assertThat(readyTopic.readKeyValuesToList()).containsExactly(
                KeyValue.pair("BAL-1", balance("BAL-1", "FA-1", 100)),
                KeyValue.pair("BAL-1", balance("BAL-1", "FA-1", 110)),
                KeyValue.pair("BAL-2", balance("BAL-2", "FA-1", 200)));

        parentTopic.pipeInput("FA-10", account("FA-10"));

        assertThat(readyTopic.readKeyValuesToList())
                .containsExactly(KeyValue.pair("BAL-9", balance("BAL-9", "FA-10", 999)));
    }

    @Test
    @DisplayName("A repeated parent success record does not release children again")
    void repeatedParentReleasesNothing() {
        childTopic.pipeInput("BAL-1", balance("BAL-1", "FA-1", 100));
        parentTopic.pipeInput("FA-1", account("FA-1"));
        parentTopic.pipeInput("FA-1", account("FA-1"));

        assertThat(readyTopic.readValuesToList()).hasSize(1);
    }

    @Test
    @DisplayName("A child that waits longer than max-wait goes to the error topic")
    void expiredChildGoesToErrorTopic() {
        childTopic.pipeInput("BAL-1", balance("BAL-1", "FA-1", 100));

        testDriver.advanceWallClockTime(MAX_WAIT.minusSeconds(30));
        assertThat(errorTopic.isEmpty()).isTrue();

        testDriver.advanceWallClockTime(Duration.ofSeconds(30));
        TestRecord<String, String> error = errorTopic.readRecord();
        assertThat(error.key()).isEqualTo("BAL-1");
        assertThat(error.value()).isEqualTo(balance("BAL-1", "FA-1", 100));
        assertThat(header(error, PreJoinProcessor.REASON_HEADER)).isEqualTo(PreJoinProcessor.REASON_EXPIRED);
        assertThat(header(error, PreJoinProcessor.PARENT_ID_HEADER)).isEqualTo("FA-1");

        // The expired child is gone from the store: a late parent does not release it
        parentTopic.pipeInput("FA-1", account("FA-1"));
        assertThat(readyTopic.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("A child without a parent ID goes to the error topic at once")
    void childWithoutParentIdGoesToErrorTopic() {
        childTopic.pipeInput("BAL-1", "{\"balanceId\":\"BAL-1\",\"amount\":100}");
        childTopic.pipeInput("BAL-2", "not json");

        assertThat(errorTopic.readRecordsToList())
                .extracting(TestRecord::key, r -> header(r, PreJoinProcessor.REASON_HEADER))
                .containsExactly(
                        tuple("BAL-1", PreJoinProcessor.REASON_NO_PARENT_ID),
                        tuple("BAL-2", PreJoinProcessor.REASON_NO_PARENT_ID));
        assertThat(readyTopic.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("A parent without a parent ID goes to the parent error topic")
    void parentWithoutParentIdGoesToParentErrorTopic() {
        childTopic.pipeInput("BAL-1", balance("BAL-1", "FA-1", 100));
        parentTopic.pipeInput("FA-1", "{\"id\":\"FA-1\",\"name\":\"Account FA-1\"}");
        parentTopic.pipeInput("FA-2", "not json");

        assertThat(parentErrorTopic.readRecordsToList())
                .extracting(TestRecord::key, TestRecord::value, r -> header(r, PreJoinProcessor.REASON_HEADER))
                .containsExactly(
                        tuple("FA-1", "{\"id\":\"FA-1\",\"name\":\"Account FA-1\"}", PreJoinProcessor.REASON_NO_PARENT_ID),
                        tuple("FA-2", "not json", PreJoinProcessor.REASON_NO_PARENT_ID));
        // The parent was not recognized, so the child still waits
        assertThat(readyTopic.isEmpty()).isTrue();
        assertThat(errorTopic.isEmpty()).isTrue();
    }

    private static String account(String externalId) {
        return "{\"externalId\":\"" + externalId + "\",\"name\":\"Account " + externalId + "\"}";
    }

    private static String balance(String balanceId, String financialAccountId, int amount) {
        return "{\"balanceId\":\"" + balanceId + "\",\"financialAccountId\":\"" + financialAccountId
                + "\",\"amount\":" + amount + "}";
    }

    private static String header(TestRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
