package demo.kafka.integration;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import demo.kafka.KafkaDemoConfiguration;
import demo.kafka.event.DemoInboundEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.backoff.FixedBackOff;

import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static demo.kafka.util.TestEventData.buildDemoInboundEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test to demonstrate the difference in behaviour between producing events using the Kafka Transactions API
 * and producing events without transactions.
 */
@Slf4j
@SpringBootTest(classes = { KafkaDemoConfiguration.class } )
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true, count=3, topics = { "demo-transactional-inbound-topic", "demo-non-transactional-inbound-topic" })
public class KafkaTransactionsIntegrationTest extends IntegrationTestBase {

    final static String DEMO_TRANSACTIONAL_TEST_TOPIC = "demo-transactional-inbound-topic";
    final static String DEMO_NON_TRANSACTIONAL_TEST_TOPIC = "demo-non-transactional-inbound-topic";

    @Autowired
    private KafkaTestListenerReadCommitted1 testReceiverReadCommitted1;

    @Autowired
    private KafkaTestListenerReadCommitted2 testReceiverReadCommitted2;

    @Autowired
    private KafkaTestListenerReadUncommitted1 testReceiverReadUncommitted1;

    @Autowired
    private KafkaTestListenerReadUncommitted2 testReceiverReadUncommitted2;

    @Configuration
    static class TestConfig {

        @Bean
        public KafkaTestListenerReadCommitted1 testReceiverReadCommitted1() {
            return new KafkaTestListenerReadCommitted1();
        }

        @Bean
        public KafkaTestListenerReadCommitted2 testReceiverReadCommitted2() {
            return new KafkaTestListenerReadCommitted2();
        }

        @Bean
        public KafkaTestListenerReadUncommitted1 testReceiverReadUncommitted1() {
            return new KafkaTestListenerReadUncommitted1();
        }

        @Bean
        public KafkaTestListenerReadUncommitted2 testReceiverReadUncommitted2() {
            return new KafkaTestListenerReadUncommitted2();
        }

        /**
         * Will only consume messages that have been committed to the topic.
         */
        @Bean
        public ConsumerFactory<Object, Object> testConsumerFactoryReadCommitted(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaTransactionsIntegrationTest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
            return new DefaultKafkaConsumerFactory<>(config);
        }

        /**
         * Will consume messages that have not been committed to the topic.
         */
        @Bean
        public ConsumerFactory<Object, Object> testConsumerFactoryReadUncommitted(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaTransactionsIntegrationTest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT));
            return new DefaultKafkaConsumerFactory<>(config);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerReadCommittedContainerFactory(final ConsumerFactory<Object, Object> testConsumerFactoryReadCommitted) {
            final SeekToCurrentErrorHandler errorHandler =
                    new SeekToCurrentErrorHandler((record, exception) -> {
                        // 4 seconds pause, 4 retries.
                    }, new FixedBackOff(4000L, 4L));
            final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
            factory.setConsumerFactory(testConsumerFactoryReadCommitted);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
            factory.setErrorHandler(errorHandler);
            return factory;
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerReadUncommittedContainerFactory(final ConsumerFactory<Object, Object> testConsumerFactoryReadUncommitted) {
            final SeekToCurrentErrorHandler errorHandler =
                    new SeekToCurrentErrorHandler((record, exception) -> {
                        // 4 seconds pause, 4 retries.
                    }, new FixedBackOff(4000L, 4L));
            final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
            factory.setConsumerFactory(testConsumerFactoryReadUncommitted);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
            factory.setErrorHandler(errorHandler);
            return factory;
        }

        /**
         * A non-transactional producer for the test.
         */
        @Bean
        public ProducerFactory<String, String> testProducerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            Properties config = new Properties();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            log.debug("Test Kafka producer is created with brokerUrl={}", bootstrapServers);
            return new DefaultKafkaProducerFactory(config);
        }

        @Bean
        public KafkaTemplate<String, String> testKafkaTemplate(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            return new KafkaTemplate<>(testProducerFactory(bootstrapServers));
        }
    }

    public static class KafkaTestListenerReadCommitted1 {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaTransactionsIntegrationTestReadCommitted", topics = "demo-outbound-topic1", containerFactory = "kafkaListenerReadCommittedContainerFactory", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListener1 - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    public static class KafkaTestListenerReadCommitted2 {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaTransactionsIntegrationTestReadCommitted", topics = "demo-outbound-topic2", containerFactory = "kafkaListenerReadCommittedContainerFactory", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListener2 - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    public static class KafkaTestListenerReadUncommitted1 {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaTransactionsIntegrationTestReadUncommitted", topics = "demo-outbound-topic1", containerFactory = "kafkaListenerReadUncommittedContainerFactory", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListenerReadUncommitted1 - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    public static class KafkaTestListenerReadUncommitted2 {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaTransactionsIntegrationTestReadUncommitted", topics = "demo-outbound-topic2", containerFactory = "kafkaListenerReadUncommittedContainerFactory", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListenerReadUncommitted2 - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp() {
        super.setUp();
        testReceiverReadCommitted1.counter.set(0);
        testReceiverReadCommitted2.counter.set(0);
        testReceiverReadUncommitted1.counter.set(0);
        testReceiverReadUncommitted2.counter.set(0);
    }

    /**
     * Prove the happy path send and receive is working with no transactions.
     *
     * One event sent by the test is consumed by the application and two outbound events are produced to different topics.
     *
     * The two test receives then consume these events.
     */
    @Test
    public void testNonTransactionalSuccess() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        stubWiremock("/api/kafkatransactionsdemo/" + key, 200, "Success");

        sendMessage(DEMO_NON_TRANSACTIONAL_TEST_TOPIC, eventId, key, buildDemoInboundEvent(key));

        // Check for a message being emitted on demo-outbound-topic1 - read committed
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadCommitted1.counter::get, equalTo(1));

        // Check for a message being emitted on demo-outbound-topic2 - read committed
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadCommitted2.counter::get, equalTo(1));

        // Check for a message being emitted on demo-outbound-topic1 - read uncommitted
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadCommitted1.counter::get, equalTo(1));

        // Check for a message being emitted on demo-outbound-topic2 - read uncommitted
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadCommitted2.counter::get, equalTo(1));

        verify(exactly(1), getRequestedFor(urlEqualTo("/api/kafkatransactionsdemo/" + key)));
    }

    /**
     * Prove the happy path send and receive is working with transactions.
     *
     * One event sent by the test is consumed by the application and two outbound events are produced to different topics.
     *
     * The two test receives then consume these events.
     */
    @Test
    public void testTransactionalSuccess() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        stubWiremock("/api/kafkatransactionsdemo/" + key, 200, "Success");

        sendMessage(DEMO_TRANSACTIONAL_TEST_TOPIC, eventId, key, buildDemoInboundEvent(key));

        // Check for a message being emitted on demo-outbound-topic1 - read committed
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadCommitted1.counter::get, equalTo(1));

        // Check for a message being emitted on demo-outbound-topic2 - read committed
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadCommitted2.counter::get, equalTo(1));

        // Check for a message being emitted on demo-outbound-topic1 - read uncommitted
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadUncommitted1.counter::get, equalTo(1));

        // Check for a message being emitted on demo-outbound-topic2 - read uncommitted
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadUncommitted2.counter::get, equalTo(1));

        verify(exactly(1), getRequestedFor(urlEqualTo("/api/kafkatransactionsdemo/" + key)));
    }

    /**
     * The application consumes an event, and performs the following:
     *
     * 1. produce outbound event to topic1.
     * 2. call third party service.
     * 2a. this is wiremocked to initially fail.
     * 2b. the event is re-polled.
     * 3. produces outbound event to topic1 again.
     * 4. calls third party service successfully.
     * 5. produces outbound event to topic2.
     *
     * As the outbound events are not produced within a transaction, then BOTH writes to the topic1 are completed,
     * and hence the test receiver receives two events from topic1.
     */
    @Test
    public void testExactlyOnceSemantics_NonTransactionalConsumer() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        // Prime the rest api mock to return service unavailable on the first call then return a success
        stubWiremock("/api/kafkatransactionsdemo/" + key, 500, "Unavailable", "failOnce", STARTED, "succeedNextTime");
        stubWiremock("/api/kafkatransactionsdemo/" + key, 200, "success", "failOnce", "succeedNextTime", "succeedNextTime");

        DemoInboundEvent inboundEvent = buildDemoInboundEvent(key);
        sendMessage(DEMO_NON_TRANSACTIONAL_TEST_TOPIC, eventId, key, inboundEvent);

        // Check for a message being emitted on demo-outbound-topic1
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadCommitted1.counter::get, equalTo(2));

        // Now check the exactly once semantics have not been respected, as no transaction was used.
        TimeUnit.SECONDS.sleep(5);
        assertThat(testReceiverReadCommitted1.counter.get(), equalTo(2));
        assertThat(testReceiverReadCommitted2.counter.get(), equalTo(1));
        assertThat(testReceiverReadUncommitted1.counter.get(), equalTo(2));
        assertThat(testReceiverReadUncommitted2.counter.get(), equalTo(1));
        verify(exactly(2), getRequestedFor(urlEqualTo("/api/kafkatransactionsdemo/" + key)));
    }

    /**
     * The application consumes an event, and performs the following:
     *
     * 1. produce outbound event to topic1.
     * 2. call third party service.
     * 2a. this is wiremocked to initially fail.
     * 2b. the event is re-polled.
     * 3. produces outbound event to topic1 again.
     * 4. calls third party service successfully.
     * 5. produces outbound event to topic2.
     *
     * As the outbound events are produced within a transaction, then only the second write to the topic1 is committed
     * once the service call transaction completes. Hence the test receiver only receives the one event from topic1.
     */
    @Test
    public void testExactlyOnceSemantics_TransactionalConsumer() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        // Prime the rest api mock to return service unavailable on the first call then return a success
        stubWiremock("/api/kafkatransactionsdemo/" + key, 500, "Unavailable", "failOnce", STARTED, "succeedNextTime");
        stubWiremock("/api/kafkatransactionsdemo/" + key, 200, "success", "failOnce", "succeedNextTime", "succeedNextTime");

        DemoInboundEvent inboundEvent = buildDemoInboundEvent(key);
        sendMessage(DEMO_TRANSACTIONAL_TEST_TOPIC, eventId, key, inboundEvent);

        // Check for a message being emitted on demo-outbound-topic1
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiverReadCommitted1.counter::get, equalTo(1));

        // Now check the exactly once semantics have been respected, as a transaction was used.
        TimeUnit.SECONDS.sleep(5);
        assertThat(testReceiverReadCommitted1.counter.get(), equalTo(1));
        assertThat(testReceiverReadCommitted2.counter.get(), equalTo(1));

        // The read uncommitted receivers will still receive the re-produced event to topic 1.
        assertThat(testReceiverReadUncommitted1.counter.get(), equalTo(2));
        assertThat(testReceiverReadUncommitted2.counter.get(), equalTo(1));
        verify(exactly(2), getRequestedFor(urlEqualTo("/api/kafkatransactionsdemo/" + key)));
    }
}
