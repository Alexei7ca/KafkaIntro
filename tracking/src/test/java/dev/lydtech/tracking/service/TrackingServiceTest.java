package dev.lydtech.tracking.service;

import dev.lydtech.tracking.message.DispatchCompleted;
import dev.lydtech.tracking.message.DispatchPreparing;
import dev.lydtech.tracking.message.TrackingStatus;
import dev.lydtech.tracking.message.TrackingStatusUpdated;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TrackingServiceTest {

    private TrackingService trackingService;
    private KafkaTemplate<String, Object> kafkaProducerMock;

    @BeforeEach
    void setUp() {
        kafkaProducerMock = mock(KafkaTemplate.class);
        trackingService = new TrackingService(kafkaProducerMock);
    }

    @Test
    void process_DispatchPreparing_Success() throws Exception {
        when(kafkaProducerMock.send(anyString(), any(TrackingStatusUpdated.class))).thenReturn(mock(CompletableFuture.class));

        UUID orderId = UUID.randomUUID();
        DispatchPreparing dispatchPreparing = DispatchPreparing.builder().orderId(orderId).build();
        trackingService.process(dispatchPreparing);

        verify(kafkaProducerMock, times(1)).send(eq("tracking.status"), any(TrackingStatusUpdated.class));
    }

    @Test
    void process_DispatchCompleted_Success() throws Exception {
        when(kafkaProducerMock.send(anyString(), any(TrackingStatusUpdated.class))).thenReturn(mock(CompletableFuture.class));

        UUID orderId = UUID.randomUUID();
        DispatchCompleted dispatchCompleted = DispatchCompleted.builder().orderId(orderId).date("2026-09-27T10:00:00Z").build();
        trackingService.process(dispatchCompleted);

        verify(kafkaProducerMock, times(1)).send(eq("tracking.status"), any(TrackingStatusUpdated.class));
    }

    @Test
    void process_ProducerThrowsException() {
        when(kafkaProducerMock.send(anyString(), any(TrackingStatusUpdated.class))).thenThrow(new RuntimeException("Producer failure"));

        DispatchPreparing dispatchPreparing = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();
        Exception exception = assertThrows(RuntimeException.class, () -> trackingService.process(dispatchPreparing));

        assertThat(exception.getMessage(), equalTo("Producer failure"));
    }
}
