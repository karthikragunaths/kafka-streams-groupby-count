package kafka.learning;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
@Component
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        kafkaTemplate.send("groupby-count-input", message)
          .addCallback(
            result -> log.info("Message sent to topic: {}", message),
            ex -> log.error("Failed to send message", ex)
          );
    }
}
