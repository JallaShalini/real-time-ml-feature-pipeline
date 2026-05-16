# Real-Time ML Feature Pipeline

Quick start (requires Docker & Docker Compose):

```bash
docker-compose up --build -d
# create topics if not already created
./scripts/create_topics.sh
```

Run integration test:

```bash
docker-compose exec -T flink-job bash -lc "python tests/integration_test.py"
```

Generate batch vs stream comparison (writes `analysis/comparison_report.json`):

```bash
docker-compose exec -T flink-job bash -lc "python analysis/batch_compute.py"
```

Package for submission:

```powershell
powershell Compress-Archive -Path . -DestinationPath submission.zip
```

Files of interest:
- `flink-job/main.py` — stream processor (event-time emulation)
- `dashboard/` — Flask + Socket.IO dashboard
- `analysis/` — analysis scripts and reports
- `tests/integration_test.py` — simple integration check
# Real-Time ML Feature Pipeline

Scaffold for a real-time feature engineering pipeline using Kafka and Apache Flink.

Follow the Project Development Roadmap in the repository root to build and run the system.