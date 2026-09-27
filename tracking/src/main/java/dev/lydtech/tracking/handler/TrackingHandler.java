package dev.lydtech.tracking.handler;

import dev.lydtech.tracking.message.DispatchCompleted;
import dev.lydtech.tracking.message.DispatchPreparing;
import dev.lydtech.tracking.service.TrackingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
@KafkaListener(id = "trackingConsumerClient", topics = "dispatch.tracking", groupId = "tracking.dispatch.tracking.consumer")
public class TrackingHandler {

    private final TrackingService trackingService;

    @KafkaHandler
    public void listen(DispatchPreparing payload) {
        log.info("Received DispatchPreparing: {}", payload);
        try {
            trackingService.process(payload);
        } catch (Exception e) {
            log.error("Processing failure", e);
        }
    }

    @KafkaHandler
    public void listen(DispatchCompleted payload) {
        log.info("Received DispatchCompleted: {}", payload);
        try {
            trackingService.process(payload);
        } catch (Exception e) {
            log.error("Processing failure", e);
        }
    }
}
