package demo.kafka.service;

import demo.kafka.event.DemoInboundEvent;
import demo.kafka.exception.KafkaDemoException;
import demo.kafka.exception.KafkaDemoRetryableException;
import demo.kafka.lib.KafkaClient;
import demo.kafka.properties.KafkaDemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
@RequiredArgsConstructor
public class DemoService {;

    @Autowired
    private KafkaDemoProperties properties;

    @Autowired
    private KafkaClient kafkaClient;

    /**
     * With no transaction in place:
     *
     * 1. Sends an outbound event.
     * 2. Calls a third party service.
     * 3. Sends a second outbound event.
     *
     * A configurable wiremock is used for the third party service  to optionally enable a retry between the publishing
     * of the two outbound events.
     */
    public void processWithoutTransaction(String key, DemoInboundEvent event) {
        kafkaClient.sendMessageWithoutTransaction(key, event.getData(), properties.getOutboundTopic1());
        callThirdparty(key);
        kafkaClient.sendMessageWithoutTransaction(key, event.getData(), properties.getOutboundTopic2());
    }

    /**
     * With a transaction in place:
     *
     * 1. Sends an outbound event.
     * 2. Calls a third party service.
     * 3. Sends a second outbound event.
     *
     * A configurable wiremock is used for the third party service  to optionally enable a retry between the publishing
     * of the two outbound events.
     */
    @Transactional
    public void processWithTransaction(String key, DemoInboundEvent event) {
        kafkaClient.sendMessageWithTransaction(key, event.getData(), properties.getOutboundTopic1());
        callThirdparty(key);
        kafkaClient.sendMessageWithTransaction(key, event.getData(), properties.getOutboundTopic2());
    }

    private void callThirdparty(String key) {
        RestTemplate restTemplate = new RestTemplate();
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(properties.getThirdpartyEndpoint() + "/" + key, String.class);
            if (response.getStatusCodeValue() != 200) {
                throw new RuntimeException("error " + response.getStatusCodeValue());
            }
            return;
        } catch (HttpServerErrorException e) {
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ") with an error code of " + e.getRawStatusCode(), e);   // e.getRawStatusCode()
            throw new KafkaDemoRetryableException(e);
        } catch (ResourceAccessException e) {
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ")", e);
            throw new KafkaDemoRetryableException(e);
        } catch (Exception e) {
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ")", e);
            throw new KafkaDemoException(e);
        }
    }
}
