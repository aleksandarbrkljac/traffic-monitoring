# Real-Time Traffic Monitoring and Speeding Violation Detection System

This project provides a solution for real-time traffic monitoring and speeding violation detection. It processes data about a car's location and speed, identifies instances of speeding, and stores data about traffic and speeding violations in separate databases.

## System Components

1. **Traffic Data Producer:** Simulates car traffic data and sends it to a Kafka topic. The simulated data includes attributes such as location (latitude, longitude), speed, and an identifier.

2. **Data Processor (Apache Spark Streaming):** Consumes the data from the Kafka topic, processes it to identify violations, and stores the results in appropriate databases.

3. **InfluxDB Database:** Stores time-series traffic data.

4. **PostgreSQL Database:** Stores data about speeding violations.

## Implementation Details

### Traffic Data Producer

Generates simulated car data with attributes such as location (latitude, longitude), speed, and identifier. This data is sent to a Kafka Topic to simulate traffic. This is implemented in `producer.py`.

### Apache Spark Streaming Data Processor

1. **Data Consumption:** The Spark Streaming session consumes data from the Kafka topic.

2. **Data Schema:** A schema for the data being read from Kafka topics is defined.

3. **Speed Limit Identification:** The system identifies the speed limit on the road where the car is located using data from `road_segments.csv`.

4. **Violation Detection:** The system determines if a car is speeding or has stopped. It adds a column indicating if the speed limit has been exceeded for each data point. Another column is added to indicate if the car has stopped (i.e., the speed is zero).

5. **Data Storage:** Traffic data is stored in InfluxDB, a time-series database. Speeding violation data is stored in a PostgreSQL relational database.

This is implemented in `consumer.py`. The `haversine` function calculates the distance based on coordinates. The `process_batch` function processes each batch of data, identifies violations, and writes the data to the appropriate databases. Separate functions are used to write data to InfluxDB (`write_to_infuxdb`) and PostgreSQL (`write_to_postgres`).

## Usage

### Start the Consumer
```
    docker exec -it consumer /bin/bash
    python consumer.py
```

### Start the Producer

```
    docker exec -it producer /bin/bash
    python producer.py
```

### Subscribe to Kafka Topic

```
    /bin$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_kafka_topic
```

## Additional Resources

Use this [website](https://gpx.studio/) to generate a car's driving route.