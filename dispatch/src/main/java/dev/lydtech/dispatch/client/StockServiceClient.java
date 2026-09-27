package dev.lydtech.dispatch.client;

import dev.lydtech.dispatch.exception.NotRetryableException;
import dev.lydtech.dispatch.exception.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Component
public class StockServiceClient {

    private final RestTemplate restTemplate;
    private final String stockServiceEndpoint;

    public StockServiceClient(@Autowired RestTemplate restTemplate, @Value("${dispatch.stockServiceEndpoint}") String stockServiceEndpoint) {
        this.restTemplate = restTemplate;
        this.stockServiceEndpoint = stockServiceEndpoint;
    }
    public String checkAvailability(String item) {
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(stockServiceEndpoint + "?item=" + item, String.class);
            if (response.getStatusCode().value() != 200) {
                throw new RuntimeException("error " + response.getStatusCode().value());
            }
            return response.getBody();
        } catch (HttpServerErrorException e) {
            log.error("Server exception error code: " + e.getStatusCode().value(), e);
            throw new RetryableException(e);
        } catch (ResourceAccessException e) {
            log.error("Resource access exception.", e);
            throw new RetryableException(e);
        } catch (HttpClientErrorException e) {
            log.error("Client exception error code: " + e.getStatusCode().value(), e);
            throw new NotRetryableException(e);
        } catch (Exception e) {
            log.error("Exception thrown: " + e.getClass().getName(), e);
            throw e;
        }
    }
}
