# F1 Race Results Auto-Fetcher

Automatically fetches race results from OpenF1 after each F1 race.

## How It Works

1. **Scheduler** (`scheduler.py`): Runs daily, spawns one-time CronJobs for upcoming races
2. **Fetcher** (`fetch_race_results.py`): Fetches results from OpenF1 (via `src/openf1.py`) and updates database
3. **CronJobs**: Kubernetes CronJobs that run once per race (1.5 hours after start)

## Flow

```
Race Date/Time
    ↓
+ 1.5 hours (race typically ends)
    ↓
K8s CronJob triggers
    ↓
Fetch results from OpenF1
    ↓
If results available:
   - Update database
   - Calculate scores
   - Mark race complete
   - CronJob auto-deletes after success
If not available:
   - Retry next scheduled run
```

## Manual Trigger

To manually fetch results (prod):
```bash
kubectl exec -it deployment/f1-predictor -n f1-predictor -- \
  python3 /app/cron/fetch_race_results.py
```

For dev: use namespace `f1-predictor-dev`.

## API Source

Uses [OpenF1](https://openf1.org/) via `src/openf1.py` - the single client all
cron jobs and app reads go through. Free, no-auth tier; see `src/openf1.py`
for retry/cache behaviour.
