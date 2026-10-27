# Flink Real-Time E-Commerce Analytics

A local Windows demo that streams rows from the Online Retail Excel dataset into Kafka and uses Apache Flink to calculate one-minute event-time sales totals by country.

## What was fixed

- Sales aggregation now reads `validOrders`; cancelled or otherwise invalid order rows no longer contribute to totals.
- Malformed JSON is sent to an `INVALID JSON` side output instead of continuously restarting the Flink job.
- The Flink TaskManager is configured with two task slots to match the Java job's parallelism of two.
- Docker Compose waits for Kafka to be healthy before starting the Flink JobManager.
- The startup script creates the `orders` Kafka topic and submits the freshly built JAR automatically.
- Flink and its Docker images use the same Flink 1.20.5 version, with Java 17.
- Python dependencies are recorded in `producer/requirements.txt`.

## Prerequisites on Windows

Install and verify these tools first:

```cmd
java -version
javac -version
mvn -version
docker --version
docker compose version
```

Use a JDK 17 installation for the project. Docker Desktop must be running and configured to use its WSL 2 backend. Python 3 is required only for the event producer.

## Run the project

1. Extract the whole project folder, keeping `data\Online Retail.xlsx` in place.
2. Open Command Prompt in the project folder (the folder containing `pom.xml`).
3. Run:

   ```cmd
   start-project.cmd
   ```

   The script builds the Java application, starts Kafka and Flink, creates the Kafka topic, and submits the Flink job. The first run may take longer while Docker downloads the images and Maven downloads Java dependencies.
4. Open a **second** Command Prompt in the same project folder and run:

   ```cmd
   run-producer.cmd
   ```

   This installs the Python dependencies if required and streams 10,000 dataset rows at a 20 ms delay. The stream can be stopped with `Ctrl+C`.
5. Open the Flink dashboard at <http://localhost:8081>. To view the job's console output, run:

   ```cmd
   docker compose logs -f taskmanager
   ```

   You should see `REAL-TIME SALES` output for completed one-minute event-time windows, plus `INVALID ORDER` entries for cancelled/invalid rows.

## Useful commands

```cmd
:: Check running containers
docker compose ps

:: Follow Flink logs
docker compose logs -f jobmanager taskmanager

:: Follow Kafka logs
docker compose logs -f kafka

:: List Kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

:: Stop the stack
docker compose down
```

## Troubleshooting

- If `start-project.cmd` says Maven is missing, open a new terminal and check `mvn -version`; correct Maven's PATH before continuing.
- If Docker commands fail, start Docker Desktop and wait for the engine to finish starting.
- If the job is running but there are no sales yet, start `run-producer.cmd` in the second terminal. Sales are emitted when event-time windows close, so output is not necessarily printed for every single event.
- If the producer cannot connect to Kafka, check `docker compose ps` and `docker compose logs kafka`. The producer uses `localhost:29092`; Flink connects to `kafka:9092` inside the Compose network.
- The app uses the included Excel dataset by default. Keep the dataset file at `data\Online Retail.xlsx`, or pass a different path with `--file`.

This is a local demonstration setup, not a production deployment. Each run of `start-project.cmd` first stops any previous demo containers for a clean run. Kafka data is stored in the container filesystem and is removed when the containers are recreated.
