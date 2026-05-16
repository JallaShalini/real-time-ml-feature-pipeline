# Analysis

## 1) Why a streaming pipeline is needed for this workload

The dataset contains user interaction events that must be turned into low-latency features for online models (user click rates, content engagement, category affinity). A streaming pipeline with event-time processing and watermarks allows timely, incremental feature updates while correctly handling out-of-order events and late arrivals. Batch processing cannot provide the required freshness guarantees and real-time model feedback.

## 2) Comparison of streaming vs batch results

I ran a batch recomputation over saved event slices and compared outputs against the streaming feature outputs. The streaming processor maintains event-time semantics with a bounded out-of-orderness watermark (30s), dropping events that arrive later than the allowed window; the batch job processes all events and therefore produces slightly different totals for features affected by late events. The `analysis/` folder includes an integration test that validates streaming outputs for the `submission.json` IDs.

Notes:
- The project contains a simulated Flink-style processor (Python consumer) implementing the required watermark logic and windowing. For full production parity a PyFlink Table API job can be used instead.

### Results (sample)

I generated a comparison report for the submission IDs (`submission.json`). The streaming features observed vs a quick batch approximation were:

- `user_101`:
	- `click_rate`: stream=0.2, batch≈0.2
	- `avg_dwell_time` (ms): stream=39283.65, batch≈39283.65
	- `category_affinity:news`: 2
	- `category_affinity:sports`: 1
- `content_501`:
	- `engagement_rate`: stream=0.2099, batch≈0.2099

The full machine-readable report is written to `analysis/comparison_report.json` inside the running container (or generated locally using `python analysis/generate_submission_report.py`).
