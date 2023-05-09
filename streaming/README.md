To start execution of consumer use command

```
    docker exec -it consumer /bin/bash
    python consumer.py
```

To start execution of consumer use command

```
    docker exec -it producer /bin/bash
    python producer.py
```

Subscribe to kafka topic cli

```
    /bin$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_kafka_topic
```

[Website to generate route of driving car](https://gpx.studio/)

# Sistem za praćenje saobraćaja u realnom vremenu i kažnjavanje prekoračenja brzine

Razviti sistem za praćenje saobraćaja u realnom vremenu koji obrađuje podatke o lokaciji i brzini automobila, identifikuje slučajeve prekoračenja brzine i skladišti podatke o saobraćaju i kaznama za prekoračenje brzine u zasebnim bazama podataka.

1. Implementirati proizvođača za simulaciju saobraćaja automobila i slanje podataka na Kafka Topic

   1. Generisanje simuliranih podataka o automobilima sa atributima kao što su lokacija (latitude, longitude), brzina, identifikator. (route.json)
   2. Slanje podataka na Kafka Topic da bi simulirali saobraćaj

2. Slušanje Kafka topic sa Apache Spark Streaming-om i obrada podataka

   1. Kreiranje Spark Streaming sesije za konzumiranje podataka iz Kafka topica
   2. Definisanje šeme za podatke koji se čitaju sa Kakfa topica

3. Odrediti ograničenje na putu na kojem se nalazi automobil (road_segments.csv)

4. Utvrditi da li automobil prekoračuje ograničenje brzine ili je zaustavljen

   1. Dodavanje kolone koja ukazuje na prekoračenje ograničenja brzine za svaku tačku podataka automobila.
   2. Dodavanje još jedne kolone koja ukazuje da li je automobil zaustavljen (tj. brzina je jednaka nuli).

5. Podatke o saobraćaju čuvati u time-series bazi (npr. InfluxDB)

6. Podatke o prekoračenju brzine čuvati u relacionoj bazi (npr. PostgreSQL)
