# TP Kafka Mini-Projet

## Description

Ce projet est une simulation d'un système de point de vente (POS) utilisant Apache Kafka pour traiter les événements en temps réel. Il démontre l'utilisation de Kafka pour la messagerie asynchrone dans un environnement de commerce électronique, en générant des événements simulés et en les traitant pour calculer des métriques et détecter des anomalies.

Le projet utilise Java 8, Apache Kafka pour la communication, et Jackson pour la sérialisation/désérialisation JSON des événements.

## Fonctionnalités

- **Simulation de caisses** : Génération aléatoire d'événements de vente, retour et ouverture de caisse.
- **Calcul du chiffre d'affaires** : Agrégation en temps réel du CA par ville.
- **Détection d'anomalies** : Alerte automatique pour les retours supérieurs à 200 DT.
- **Logs avec emojis** : Affichage coloré et intuitif des événements.

## Architecture

L'architecture repose sur le pattern producteur-consommateur de Kafka :

1. **SimulateurCaisse** (Producteur) → Publie des événements sur le topic `pos-events`.
2. **ChiffreAffairesParVille** (Consommateur) → Consomme `pos-events`, calcule le CA par ville et affiche périodiquement.
3. **DetecteurAnomalies** (Consommateur/Producteur) → Consomme `pos-events`, détecte anomalies et publie sur `alertes-retours`.

### Topics Kafka
- `pos-events` : Événements principaux (ventes, retours, ouvertures). Partitionné pour la scalabilité.
- `alertes-retours` : Alertes pour retours anormaux.

### Modèle de données
Classe `Event` :
- `type` : "VENTE", "RETOUR", "OUVERTURE"
- `ville` : Ville de la caisse
- `montant` : Montant de la transaction (null pour ouvertures)
- `idCaisse` : Identifiant unique de la caisse
- `timestamp` : Horodatage ISO
- `produits` : Liste des produits (simulée)

### Flux de données
```
SimulateurCaisse → pos-events → ChiffreAffairesParVille (calcul CA)
                          → DetecteurAnomalies → alertes-retours
```

## Prérequis

- **Java** : Version 8 ou supérieure (recommandé : 11+ pour de meilleures performances).
- **Apache Kafka** : Version 3.7.0 ou compatible (testé avec 3.7.0).
- **Maven** : Pour la compilation et la gestion des dépendances (version 3.6+).
- **Système d'exploitation** : Linux/Mac/Windows avec terminal bash.

Assurez-vous que Kafka est configuré et en cours d'exécution sur `localhost:9092`.

## Installation

1. **Clonez le dépôt** :
   ```bash
   git clone <url-du-depot>
   cd tp-kafka-mini_projet
   ```

2. **Compilez le projet** :
   ```bash
   mvn clean compile
   ```
   Cela télécharge les dépendances et compile les classes Java.

## Configuration Kafka

### Démarrage de Kafka

1. Démarrez Zookeeper :
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```

2. Démarrez le serveur Kafka :
   ```bash
   bin/kafka-server-start.sh config/server.properties
   ```

### Création des topics

Créez les topics nécessaires (ajustez les partitions et le facteur de réplication selon votre environnement) :

```bash
# Topic principal pour les événements POS
bin/kafka-topics.sh --create --topic pos-events --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1

# Topic pour les alertes
bin/kafka-topics.sh --create --topic alertes-retours --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
```

Vérifiez la création :
```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Utilisation

Lancez les applications dans l'ordre suivant pour une démonstration complète :

1. **Simulateur de caisse** (en arrière-plan) :
   ```bash
   mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimulateurCaisse" &
   ```
   Génère des événements toutes les 100-500 ms.

2. **Calculateur de CA** (en arrière-plan) :
   ```bash
   mvn exec:java -Dexec.mainClass="tn.utm.kafka.ChiffreAffairesParVille" &
   ```
   Affiche le CA par ville toutes les 5 secondes.

3. **Détecteur d'anomalies** (en arrière-plan) :
   ```bash
   mvn exec:java -Dexec.mainClass="tn.utm.kafka.DetecteurAnomalies" &
   ```
   Surveille les retours > 200 DT.

### Arrêt des applications

Utilisez `Ctrl+C` ou `kill <PID>` pour arrêter chaque processus.

### Test rapide

Pour un test simple, lancez seulement le simulateur et observez les logs :
```bash
mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimulateurCaisse"
```

## Structure du projet

```
tp-kafka-mini_projet/
├── pom.xml                          # Configuration Maven
├── src/main/java/tn/utm/kafka/
│   ├── ChiffreAffairesParVille.java  # Consommateur CA
│   ├── DetecteurAnomalies.java       # Consommateur/Producteur alertes
│   ├── EventEmojis.java              # Constantes emojis
│   ├── KafkaConfig.java              # Configuration Kafka
│   ├── SimulateurCaisse.java         # Producteur événements
│   └── model/
│       └── Event.java                # Modèle de données
└── target/                           # Artefacts compilés
```

## Dépendances

Le projet utilise les dépendances suivantes (vérifiées dans `pom.xml`) :

- **Apache Kafka Clients** (3.7.0) : Bibliothèque cliente pour produire et consommer des messages Kafka. Version choisie pour la stabilité et la compatibilité avec Java 8.
- **Jackson Databind** (2.17.0) : Pour la sérialisation/désérialisation JSON des objets `Event`. Version récente pour la sécurité et les performances.

Ces dépendances sont automatiquement gérées par Maven. Pour vérifier les versions :
```bash
mvn dependency:tree
```

## Dépannage

- **Erreur de connexion Kafka** : Vérifiez que Kafka tourne sur `localhost:9092` et que les topics existent.
- **Messages non consommés** : Assurez-vous que les groupes de consommateurs sont uniques (modifiez `GROUP_ID_CONFIG` si nécessaire).
- **Problèmes de compilation** : `mvn clean install` pour forcer la résolution des dépendances.

## Auteur

[Yesser Belhaj Ali]

## Licence

[Spécifiez la licence si applicable, ex. : MIT]

[Yesser Belhaj Ali]
