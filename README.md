# Event-Driven Kafka Microservices Architecture

A resilient, production-grade event-driven architecture built with **Java 17**, **Spring Boot 3.0.5**, **Apache Kafka**, and **Maven**. This multi-service workspace showcases enterprise-level patterns for asynchronous messaging, consumer fault tolerance, categorized error routing, dead letter recovery, and multi-event single-topic handling.

---

## Architecture Overview

The system consists of two collaborating microservices communicating over Apache Kafka:

1. **`dispatch` Service**: Ingests new orders, verifies stock availability against a downstream REST service, dispatches outbound tracking and notification events, and recovers from transient or fatal failures via backoff retries and Dead Letter Topics.
2. **`tracking` Service**: Employs a multi-event single-topic consumer to receive diverse tracking milestones (`DispatchPreparing` and `DispatchCompleted`) and emits real-time status transitions.

```
                              +-------------------------+
                              |   Stock Service (REST)  |
                              +-------------------------+
                                        ^
                                        | (HTTP Availability Check)
                                        v
+------------------+         +--------------------+         +------------------------+
|   Kafka Topic    | ------> |  Dispatch Service  | ------> |      Kafka Topic       |
|  `order.created` |         +--------------------+         |   `order.dispatched`   |
+------------------+                   |                    +------------------------+
         | (Fatal/Exhausted Retries)   |
         v                             |                    +------------------------+
+----------------------+               +------------------> |      Kafka Topic       |
|     Kafka Topic      |                                    |   `dispatch.tracking`  |
|  `order.created.DLT` |                                    | (Preparing, Completed) |
+----------------------+                                    +------------------------+
                                                                        |
                                                                        v
                                                            +------------------------+
                                                            |    Tracking Service    |
                                                            +------------------------+
                                                                        |
                                                                        v
                                                            +------------------------+
                                                            |      Kafka Topic       |
                                                            |    `tracking.status`   |
                                                            +------------------------+
```

---

## Core Enterprise Patterns & Resilience

### 1. Consumer Error Handling & Backoff Retries
- Configured via Spring Kafka's `DefaultErrorHandler` paired with a `FixedBackOff` strategy (3 retry attempts at 100ms intervals).
- Failed records are retried automatically before triggering recovery mechanisms, avoiding unnecessary consumer group rebalances.

### 2. Categorized Error Routing (Retryable vs. Non-Retryable)
- **`RetryableException`**: Represents transient failures (such as downstream HTTP 5xx responses or network timeouts via `ResourceAccessException`). Triggers consumer retry backoff.
- **`NotRetryableException`**: Represents fatal or client-side errors (such as HTTP 4xx Bad Request). Bypasses retries immediately to conserve compute resources and prevent head-of-line blocking.

### 3. Dead Letter Topic (DLT) Recovery
- Integrated via `DeadLetterPublishingRecoverer` with `KafkaTemplate<String, Object>`.
- Any message that either exhausts all retries (`RetryableException`) or throws a `NotRetryableException` is published directly to `order.created.DLT`, preserving failed payloads with original headers for offline inspection and replay.

### 4. Multi-Event Single Topic Consumption
- The `tracking` service demonstrates polymorphism across the `dispatch.tracking` topic.
- A single `@KafkaListener` at class level routes incoming messages dynamically to distinct `@KafkaHandler` methods based on the deserialized payload type (`DispatchPreparing` vs. `DispatchCompleted`).

### 5. Trusted Packages & Deserialization Security
- The `JsonDeserializer` is configured with strict trusted package validation (`dev.lydtech.*`) without setting a fallback default type, mitigating unsafe deserialization vulnerabilities across heterogeneous event payloads.

### 6. Comprehensive Embedded Integration Testing
- Tested end-to-end with Spring Kafka's `@EmbeddedKafka` broker and WireMock (`@AutoConfigureWireMock`).
- Simulates live Kafka brokers, dynamic partition assignment synchronization, and faulty downstream REST endpoints (verifying 200 OK, 400 Bad Request fast-failure to DLT, 503 retry-then-success, and 503 retry exhaustion to DLT).

---

## Project Structure

```
KafkaIntro/
├── .gitignore
├── README.md
├── dispatch/                      # Dispatch Microservice (Port 8080)
│   ├── pom.xml
│   └── src/
│       ├── main/java/dev/lydtech/dispatch/
│       │   ├── DispatchApplication.java
│       │   ├── DispatchConfiguration.java
│       │   ├── client/StockServiceClient.java
│       │   ├── exception/RetryableException.java
│       │   ├── exception/NotRetryableException.java
│       │   ├── handler/OrderCreatedHandler.java
│       │   ├── message/
│       │   └── service/DispatchService.java
│       └── test/java/dev/lydtech/dispatch/
│           ├── client/StockServiceClientTest.java
│           ├── handler/OrderCreatedHandlerTest.java
│           ├── integration/OrderDispatchIntegrationTest.java
│           └── service/DispatchServiceTest.java
└── tracking/                      # Tracking Microservice (Port 8081)
    ├── pom.xml
    └── src/
        ├── main/java/dev/lydtech/tracking/
        │   ├── TrackingApplication.java
        │   ├── TrackingConfiguration.java
        │   ├── handler/TrackingHandler.java
        │   ├── message/
        │   └── service/TrackingService.java
        └── test/java/dev/lydtech/tracking/
            ├── handler/TrackingHandlerTest.java
            ├── integration/DispatchTrackingIntegrationTest.java
            └── service/TrackingServiceTest.java
```

---

## How to Run Tests

### Prerequisites
- **Java 17**
- **Maven 3.8+** (or use the included `./mvnw` wrapper)

### Run Tests for `dispatch` Service
```bash
cd dispatch
./mvnw clean test
```

### Run Tests for `tracking` Service
```bash
cd tracking
./mvnw clean test
```

### Run All Tests Across Workspace
```bash
(cd dispatch && ./mvnw clean test) && (cd tracking && ./mvnw clean test)
```
