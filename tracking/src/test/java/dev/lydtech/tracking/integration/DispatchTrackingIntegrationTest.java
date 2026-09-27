package dev.lydtech.tracking.integration;

import dev.lydtech.tracking.TrackingConfiguration;
import dev.lydtech.tracking.message.DispatchCompleted;
import dev.lydtech.tracking.message.DispatchPreparing;
import dev.lydtech.tracking.message.TrackingStatus;
import dev.lydtech.tracking.message.TrackingStatusUpdated;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@SpringBootTest(classes = {TrackingConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
public class DispatchTrackingIntegrationTest {

    private final static String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    private final static String TRACKING_STATUS_TOPIC = "tracking.status";

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private KafkaTestListener testListener;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Configuration
    static class TestConf {

        @Bean
        public KafkaTestListener testListener() {
            return new KafkaTestListener();
        }
    }

    public static class KafkaTestListener {
        AtomicInteger trackingStatusPreparingCounter = new AtomicInteger(0);
        AtomicInteger trackingStatusCompletedCounter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaIntegrationTest", topics = TRACKING_STATUS_TOPIC)
        void receiveTrackingStatusUpdated(@Payload TrackingStatusUpdated payload) {
            log.debug("Received TrackingStatusUpdated: {}", payload);
            if (TrackingStatus.PREPARING.equals(payload.getStatus())) {
                trackingStatusPreparingCounter.incrementAndGet();
            } else if (TrackingStatus.COMPLETED.equals(payload.getStatus())) {
                trackingStatusCompletedCounter.incrementAndGet();
            }
        }
    }

    @BeforeEach
    public void setUp() {
        testListener.trackingStatusPreparingCounter.set(0);
        testListener.trackingStatusCompletedCounter.set(0);

        registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, container.getContainerProperties().getTopics().length * embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @Test
    public void testDispatchTrackingFlow() throws Exception {
        UUID orderId = UUID.randomUUID();

        DispatchPreparing dispatchPreparing = DispatchPreparing.builder()
                .orderId(orderId)
                .build();
        sendMessage(DISPATCH_TRACKING_TOPIC, dispatchPreparing);

        DispatchCompleted dispatchCompleted = DispatchCompleted.builder()
                .orderId(orderId)
                .date(Instant.now().toString())
                .build();
        sendMessage(DISPATCH_TRACKING_TOPIC, dispatchCompleted);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.trackingStatusPreparingCounter::get, equalTo(1));
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testListener.trackingStatusCompletedCounter::get, equalTo(1));
    }

    private void sendMessage(String topic, Object data) throws Exception {
        kafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build()).get();
    }
}
