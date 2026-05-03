package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import tn.utm.kafka.model.Event;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class DetecteurAnomalies {

    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        Properties cProps = new Properties();

        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP);
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "alerte-1");

        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(cProps);

        consumer.subscribe(List.of(KafkaConfig.TOPIC_EVENTS));

        Properties pProps = new Properties();

        pProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP);
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(pProps);

        System.out.println("🚨 DetecteurAnomalies démarré...");

        while (true) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(200));

            for (ConsumerRecord<String, String> r : records) {

                try {
                    Event e = mapper.readValue(r.value(), Event.class);

                    if (e == null || e.montant == null || e.type == null) continue;

                    if ("RETOUR".equals(e.type) && e.montant > 200) {

                        String alert = EventEmojis.RETOUR +
                                " ALERTE RETOUR > 200 DT | " +
                                e.ville + " | " + e.montant;

                        producer.send(new ProducerRecord<>(
                                KafkaConfig.TOPIC_ALERTES,
                                e.ville,
                                alert
                        ));

                        System.out.println(alert);
                    }

                } catch (Exception ex) {
                    System.out.println("❌ Erreur message: " + r.value());
                }
            }
        }
    }
}