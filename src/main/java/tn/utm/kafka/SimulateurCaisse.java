package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import tn.utm.kafka.model.Event;
import org.apache.kafka.clients.producer.*;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class SimulateurCaisse {

    static String[] villes = {"Tunis", "Sousse", "Sfax", "Bizerte", "Gabès"};
    static Random rand = new Random();
    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("🚀 SimulateurCaisse démarré...");

        while (true) {

            Event e = new Event();

            e.ville = villes[rand.nextInt(villes.length)];
            e.idCaisse = "CAISSE-" + e.ville + "-" + rand.nextInt(5);

            int r = rand.nextInt(100);

            if (r < 70) e.type = "VENTE";
            else if (r < 80) e.type = "RETOUR";
            else e.type = "OUVERTURE";

            e.timestamp = Instant.now().toString();

            if (e.type.equals("VENTE") || e.type.equals("RETOUR")) {
                e.montant = 5 + rand.nextDouble() * 495;
            } else {
                e.montant = null;
            }

            e.produits = List.of("pain", "lait", "fromage");

            String json = mapper.writeValueAsString(e);

            // 🎯 emoji fixe
            String emoji;
            if (e.type.equals("VENTE")) emoji = EventEmojis.VENTE;
            else if (e.type.equals("RETOUR")) emoji = EventEmojis.RETOUR;
            else emoji = EventEmojis.OUVERTURE;

            System.out.println(emoji + " " + json);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(KafkaConfig.TOPIC_EVENTS, e.ville, json);

            producer.send(record);

            Thread.sleep(100 + rand.nextInt(400));
        }
    }
}