# Real-Time ML Feature Pipeline

Production-style demo for a real-time feature engineering pipeline with Kafka, Flink, and a live dashboard.

## What is included

- Kafka topics for `user-events`, `content-metadata`, `feature-store`, and `metrics`
- Flink JobManager and TaskManager
- Python stream processor in `flink-job/main.py`
- Dashboard at `http://localhost:5000`
- Uploadable Flink example JAR at `WordCount.jar`
- Analysis and test scripts under `analysis/` and `tests/`

## Architecture

- `zookeeper` coordinates Kafka
- `kafka` stores the event streams and feature results
- `flink-jobmanager` and `flink-taskmanager` run the Flink cluster
- `flink-job` runs the Python feature pipeline container
- `producer` generates sample user/content events
- `dashboard` exposes the live polling UI

## Technologies Used

- Apache Kafka
- Apache Flink
- Python 3.10
- Flask
- Flask-SocketIO
- Docker and Docker Compose

## Screenshots

- Flink UI shows running and completed jobs after submitting `WordCount.jar`
- Dashboard polling UI is available at `http://localhost:5000/poll`

## URLs

- Dashboard: `http://localhost:5000/poll`
- Flink UI: `http://localhost:8081`
- Kafka: `localhost:9092`

## Run the project

```bash
docker-compose up --build
```

## Submit a Flink job

Upload `WordCount.jar` from the Flink UI at `http://localhost:8081/#/submit`, or run:

```bash
docker-compose exec flink-jobmanager /opt/flink/bin/flink run -d /opt/flink/examples/streaming/WordCount.jar
```

To run the longer streaming example:

```bash
docker-compose exec flink-jobmanager /opt/flink/bin/flink run -d /opt/flink/examples/streaming/StateMachineExample.jar
```

## Verify the stack

```bash
docker-compose exec flink-jobmanager /opt/flink/bin/flink list
curl.exe -s http://localhost:8081/jobs/overview
docker ps
```

## Main files

- `flink-job/main.py` - stream processor logic
- `dashboard/` - dashboard app
- `analysis/` - analysis utilities
- `tests/integration_test.py` - integration check