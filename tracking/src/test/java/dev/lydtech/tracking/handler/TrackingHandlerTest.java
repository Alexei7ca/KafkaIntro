package dev.lydtech.tracking.handler;

import dev.lydtech.tracking.message.DispatchCompleted;
import dev.lydtech.tracking.message.DispatchPreparing;
import dev.lydtech.tracking.service.TrackingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TrackingHandlerTest {

    private TrackingHandler handler;
    private TrackingService trackingServiceMock;

    @BeforeEach
    void setUp() {
        trackingServiceMock = mock(TrackingService.class);
        handler = new TrackingHandler(trackingServiceMock);
    }

    @Test
    void listen_DispatchPreparing_Success() throws Exception {
        DispatchPreparing payload = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();
        handler.listen(payload);
        verify(trackingServiceMock, times(1)).process(eq(payload));
    }

    @Test
    void listen_DispatchCompleted_Success() throws Exception {
        DispatchCompleted payload = DispatchCompleted.builder().orderId(UUID.randomUUID()).date("2026-09-27T10:00:00Z").build();
        handler.listen(payload);
        verify(trackingServiceMock, times(1)).process(eq(payload));
    }

    @Test
    void listen_ServiceThrowsException() throws Exception {
        DispatchPreparing payload = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();
        doThrow(RuntimeException.class).when(trackingServiceMock).process(eq(payload));
        handler.listen(payload);
        verify(trackingServiceMock, times(1)).process(eq(payload));
    }
}
