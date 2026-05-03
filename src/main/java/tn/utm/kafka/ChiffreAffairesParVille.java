package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import tn.utm.kafka.model.Event;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.*;

public class ChiffreAffairesParVille {

    static Map<String, Double> ca = new HashMap<>();
    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ca-1");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(List.of(KafkaConfig.TOPIC_EVENTS));

        long lastPrint = System.currentTimeMillis();

        System.out.println("📊 ChiffreAffairesParVille démarré...");

        while (true) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(200));

            for (ConsumerRecord<String, String> r : records) {

                try {
                    Event e = mapper.readValue(r.value(), Event.class);

                    // sécurité
                    if (e == null || e.ville == null || e.type == null) continue;

                    if (!"VENTE".equals(e.type) && !"RETOUR".equals(e.type)) continue;

                    if (e.montant == null) continue;

                    ca.putIfAbsent(e.ville, 0.0);

                    if ("VENTE".equals(e.type)) {
                        ca.put(e.ville, ca.get(e.ville) + e.montant);
                    } else if ("RETOUR".equals(e.type)) {
                        ca.put(e.ville, ca.get(e.ville) - e.montant);
                    }

                } catch (Exception ex) {
                    System.out.println("❌ Erreur parsing: " + r.value());
                }
            }

            // commit manuel
            consumer.commitSync();

            // affichage toutes les 5 sec uniquement si données
            if (System.currentTimeMillis() - lastPrint > 5000) {

                if (!ca.isEmpty()) {

                    System.out.println("\n📊 === CHIFFRE D'AFFAIRES ===");

                    ca.forEach((ville, total) ->
                            System.out.printf("🏙 %s => %.2f DT%n", ville, total)
                    );

                    System.out.println("===========================\n");
                }

                lastPrint = System.currentTimeMillis();
            }
        }
    }
}