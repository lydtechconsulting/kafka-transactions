package demo.kafka.lib;

import java.util.UUID;

import demo.kafka.exception.KafkaDemoException;
import demo.kafka.properties.KafkaDemoProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaClient {
    @Autowired
    private KafkaDemoProperties properties;

    @Autowired
    private KafkaTemplate kafkaTemplateTransactional;

    @Autowired
    private KafkaTemplate kafkaTemplateNonTransactional;

    public static final String EVENT_ID_HEADER_KEY = "demo_eventIdHeader";

    public SendResult sendMessageWithTransaction(String key, String data, String outboundTopic) {
        return sendMessage("Transactional", kafkaTemplateTransactional, key, data, outboundTopic);
    }

    public SendResult sendMessageWithoutTransaction(String key, String data, String outboundTopic) {
        return sendMessage("NonTransactional", kafkaTemplateNonTransactional, key, data, outboundTopic);
    }

    public SendResult sendMessage(String type, KafkaTemplate kafkaTemplate, String key, String data, String outboundTopic) {
        try {
            String payload = "eventId: " + UUID.randomUUID() + ", instanceId: "+properties.getInstanceId()+", payload: " + data;
            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(outboundTopic, key, payload);

            final SendResult result = (SendResult) kafkaTemplate.send(record).get();
            final RecordMetadata metadata = result.getRecordMetadata();

            log.debug(String.format("Sent %s record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                    type, record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

            return result;
        } catch (Exception e) {
            log.error("Error sending message to topic: " + outboundTopic, e);
            throw new KafkaDemoException(e);
        }
    }
}
