# Employee Attendance & Analytics API

FastAPI + MongoDB (6.0+). All code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or copy .env.example to .env
python sample_seed.py        # optional: loads the nine sample documents
uvicorn app.main:app --port 8000
```
Indexes are created at startup (idempotent, no manual steps). Docs at `/docs`.

## Notes
- Instants are epoch milliseconds in the API and BSON dates (UTC) in MongoDB; dates, months and shift times are IST strings.
- Analytics read the stored derived values; sums are kept as integers (half days, hundredths of an hour) and rounded half-up only at the end.
- The day series in the trend endpoint is generated in the pipeline with `$range` (gap filling), the moving average uses `$setWindowFields`, the leaderboard rank uses `$rank`.
- `GET /admin/explain/{endpoint}` builds the query with the same functions as the real endpoint.
- Defects found in the starter: see `REVIEW.md`. Design answers: `DECISIONS.md`.
