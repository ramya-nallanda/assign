"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (a local .env is loaded for convenience; real environment variables win)

Everything lives in this one file, top to bottom:
  1. config / Mongo / indexes
  2. pure helpers (time, R1-R5 rules, rounding, serialisation)
  3. request models
  4. endpoints: system, employees, attendance
  5. aggregation-pipeline builders + analytics endpoints
  6. explain endpoint (re-uses the same builders as the real endpoints)
"""
import calendar
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, Field, StrictInt, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError, ServerSelectionTimeoutError

# --------------------------------------------------------------------------- #
# 1. Config, Mongo, indexes
# --------------------------------------------------------------------------- #
load_dotenv()  # does NOT override variables that are already set in the environment

log = logging.getLogger("attendance")

# tz_aware=True: every datetime read back from Mongo is an aware UTC datetime (no naive/local-time surprises).
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
NON_PRESENCE = ("ABSENT", "LEAVE")
ALL_STATUSES = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
GRACE_SECONDS = 10 * 60
MAX_SHIFT_SECONDS = 24 * 3600
MAX_TREND_DAYS = 92
MAX_PATCH_RETRIES = 5

MIN_MS, MAX_MS = 100_000_000_000, 4_102_444_800_000


_indexes_ready = False


def ensure_indexes() -> bool:
    """Idempotent: re-running with the same keys and names is a no-op. True once every index exists."""
    global _indexes_ready
    specs = [
        # one record per (employee, day): the punch-in race guard, the employee-monthly range scan,
        # and the "emp_code + date range, newest first" shape of GET /attendance
        (logs, [("emp_code", ASCENDING), ("date", ASCENDING)], {"unique": True, "name": "uniq_emp_date"}),
        # GET /attendance default sort (date desc, emp_code asc) with or without a date range
        (logs, [("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "date_emp"}),
        # GET /attendance?status=...  (equality, then the same sort)
        (logs, [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "status_date_emp"}),
        # late leaderboard: date range + late_minutes > 0 + emp_code, all inside the index
        (logs, [("date", ASCENDING), ("late_minutes", ASCENDING), ("emp_code", ASCENDING)], {"name": "date_late_emp"}),
        # punch-out: latest record of an employee whose punch_in <= t
        (logs, [("emp_code", ASCENDING), ("punch_in", DESCENDING)], {"name": "emp_punch_in"}),
        # employee identity: unique, enforced by the database
        (employees, [("emp_code", ASCENDING)], {"unique": True, "name": "uniq_emp_code"}),
        # GET /employees?department=  sorted by emp_code
        (employees, [("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "dept_emp"}),
        # headcount (R9): department + joined_on <= day
        (employees, [("department", ASCENDING), ("joined_on", ASCENDING)], {"name": "dept_joined"}),
        (employees, [("joined_on", ASCENDING)], {"name": "joined"}),
    ]
    failed = False
    for coll, keys, opts in specs:
        try:
            coll.create_index(keys, **opts)
        except ServerSelectionTimeoutError as exc:  # Mongo is down: do not wait 5 s per index, /health will say 503
            log.error("MongoDB not reachable, skipping index creation: %s", exc)
            return False
        except PyMongoError as exc:  # e.g. dirty seed data blocking a unique index
            log.error("could not create index %s on %s: %s", opts.get("name"), coll.name, exc)
            failed = True
    _indexes_ready = not failed
    return _indexes_ready


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Helpers
# --------------------------------------------------------------------------- #
def unprocessable(msg: str, loc: str = "body") -> HTTPException:
    """422 in FastAPI's default validation-error shape."""
    return HTTPException(422, detail=[{"loc": [loc], "msg": msg, "type": "value_error"}])


def as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    dt = as_utc(dt)
    return None if dt is None else (dt - EPOCH) // timedelta(milliseconds=1)


def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def parse_hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM strings compare correctly


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> date:
    """R1: the IST calendar date of the punch-in; for an overnight shift, a punch-in earlier than
    shift_end belongs to the shift that started the previous day."""
    ist = as_utc(punch_in).astimezone(IST)
    d = ist.date()
    if is_overnight(shift_start, shift_end) and ist.time() < parse_hhmm(shift_end):
        d -= timedelta(days=1)
    return d


def shift_bounds(day: date, shift_start: str, shift_end: str) -> tuple[datetime, datetime]:
    """Aware IST start/end of the shift that belongs to attendance date `day`."""
    start = datetime.combine(day, parse_hhmm(shift_start), tzinfo=IST)
    end_day = day + timedelta(days=1) if is_overnight(shift_start, shift_end) else day
    end = datetime.combine(end_day, parse_hhmm(shift_end), tzinfo=IST)
    return start, end


def compute_late_minutes(punch_in: datetime, day: date, shift_start: str, shift_end: str) -> int:
    """R2: late only if STRICTLY more than 10:00 after shift_start; then whole minutes since shift_start."""
    start, _ = shift_bounds(day, shift_start, shift_end)
    seconds = int((as_utc(punch_in) - start.astimezone(UTC)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600 rounded to 2 decimals HALF-UP (Decimal, not float round())."""
    seconds = int((as_utc(punch_out) - as_utc(punch_in)).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out: datetime, day: date, shift_start: str, shift_end: str) -> int:
    """R3: whole minutes from shift_end to punch_out, only if that is at least 30."""
    _, end = shift_bounds(day, shift_start, shift_end)
    seconds = int((as_utc(punch_out) - end.astimezone(UTC)).total_seconds())
    minutes = seconds // 60
    return minutes if minutes >= 30 else 0


def compute_half_day(work_hours: float) -> bool:
    """R5: based on the already ROUNDED work_hours."""
    return work_hours < 4.50


def derived_fields(status: str, punch_in, punch_out, day: date, emp: dict) -> dict:
    """The four API-computed fields for a given final state (table in DATA_MODEL.md section 3)."""
    out = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status in NON_PRESENCE or punch_in is None:
        return out
    s, e = emp["shift_start"], emp["shift_end"]
    out["late_minutes"] = compute_late_minutes(punch_in, day, s, e)
    if punch_out is not None:
        out["work_hours"] = compute_work_hours(punch_in, punch_out)
        out["overtime_minutes"] = compute_overtime(punch_out, day, s, e)
        out["half_day"] = compute_half_day(out["work_hours"])
    return out


def parse_date(value: str, loc: str) -> date:
    """Strict YYYY-MM-DD that is also a real calendar date (else 422)."""
    if len(value) != 10 or value[4] != "-" or value[7] != "-" or not (value[:4] + value[5:7] + value[8:]).isdigit():
        raise unprocessable("date must be YYYY-MM-DD", loc)
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise unprocessable("not a valid calendar date", loc)


def parse_month(value: str) -> tuple[date, date]:
    """'YYYY-MM' -> (first day, last day) of that month, else 422."""
    if len(value) != 7 or value[4] != "-" or not (value[:4] + value[5:]).isdigit():
        raise unprocessable("month must be YYYY-MM", "month")
    year, month = int(value[:4]), int(value[5:])
    if not (1 <= month <= 12 and 1 <= year <= 9999):
        raise unprocessable("month must be YYYY-MM", "month")
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def count_weekdays(start: date, end: date) -> int:
    """Mon-Fri days in [start, end] (R7, no holiday calendar)."""
    if start > end:
        return 0
    return sum(1 for i in range((end - start).days + 1) if (start + timedelta(days=i)).weekday() < 5)


# --- serialisation: DB shape -> API shape (instants become epoch ms, _id never leaves) ---
def history_out(entries: Optional[list]) -> list:
    result = []
    for h in entries or []:
        changes = {}
        for field, ch in (h.get("changes") or {}).items():
            if field in ("punch_in", "punch_out"):
                ch = {"from": dt_to_ms(ch.get("from")), "to": dt_to_ms(ch.get("to"))}
            changes[field] = ch
        result.append({"at": dt_to_ms(h.get("at")), "by": h.get("by"), "reason": h.get("reason"), "changes": changes})
    return result


def record_out(d: dict) -> dict:
    return {
        "emp_code": d["emp_code"],
        "date": d["date"],
        "status": d["status"],
        "punch_in": dt_to_ms(d.get("punch_in")),
        "punch_out": dt_to_ms(d.get("punch_out")),
        "work_hours": d.get("work_hours"),
        "late_minutes": d.get("late_minutes") or 0,
        "overtime_minutes": d.get("overtime_minutes") or 0,
        "half_day": bool(d.get("half_day", False)),
        "history": history_out(d.get("history")),
    }


def employee_out(d: dict) -> dict:
    return {
        "emp_code": d["emp_code"],
        "name": d["name"],
        "email": d["email"],
        "department": d["department"],
        "shift_start": d["shift_start"],
        "shift_end": d["shift_end"],
        "joined_on": d["joined_on"],
        "created_at": dt_to_ms(d.get("created_at")),
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 3. Models
# --------------------------------------------------------------------------- #
EpochMs = Annotated[StrictInt, Field(ge=MIN_MS, le=MAX_MS)]  # strict: rejects floats, strings, bools
HHMM = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM)
    shift_end: str = Field(default="18:30", pattern=HHMM)
    joined_on: str

    @model_validator(mode="after")
    def _check(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        parse_date(self.joined_on, "joined_on")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: Literal["PRESENT", "WFH", "ON_DUTY"] = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[ALL_STATUSES] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        # the contract has no nullable fields here; an explicit null is not "omitted"
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} must not be null")
        return self


Page = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]


# --------------------------------------------------------------------------- #
# 4. Endpoints: system, employees, attendance
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    if not _indexes_ready:  # Mongo came up after the app did: finish what startup could not
        ensure_indexes()
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        employees.insert_one(doc)  # the unique index decides; no check-then-insert
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: Page = 1, page_size: PageSize = 20):
    q = {"department": department} if department else {}
    total = employees.count_documents(q)
    cursor = employees.find(q).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, day, emp),
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date): of N simultaneous requests exactly one wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    rec = logs.find_one({"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", DESCENDING)])
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    p_in = as_utc(rec["punch_in"])
    if ts <= p_in or (ts - p_in).total_seconds() > MAX_SHIFT_SECONDS:
        raise unprocessable("punched_at must be after punch_in and within 24 hours of it", "punched_at")
    day = date.fromisoformat(rec["date"])
    d = derived_fields(rec["status"], p_in, ts, day, emp)
    d.pop("late_minutes")  # late_minutes was fixed at punch-in
    # compare-and-set: only succeeds while the record is still open, so a second simultaneous punch-out loses
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None},
        {"$set": {"punch_out": ts, **d}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


def attendance_filter(emp_code, date_from, date_to, status) -> dict:
    """Validated filter shared by GET /attendance and its explain."""
    q: dict[str, Any] = {}
    if emp_code:
        q["emp_code"] = emp_code
    lo = parse_date(date_from, "date_from") if date_from else None
    hi = parse_date(date_to, "date_to") if date_to else None
    if lo and hi and lo > hi:
        raise unprocessable("date_from must not be after date_to", "date_from")
    if lo or hi:
        q["date"] = {}
        if lo:
            q["date"]["$gte"] = lo.isoformat()
        if hi:
            q["date"]["$lte"] = hi.isoformat()
    if status:
        q["status"] = status
    return q


ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[ALL_STATUSES] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    q = attendance_filter(emp_code, date_from, date_to, status)
    total = logs.count_documents(q) if q else logs.estimated_document_count()
    cursor = logs.find(q).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    day = parse_date(date, "date")
    date_str = day.isoformat()
    emp = get_employee_or_404(emp_code)

    for _ in range(MAX_PATCH_RETRIES):
        rec = logs.find_one({"emp_code": emp_code, "date": date_str})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")
        update, guard = plan_regularization(rec, emp, day, body)
        updated = logs.find_one_and_update(guard, update, return_document=ReturnDocument.AFTER)
        if updated is not None:
            return record_out(updated)
        # someone else changed the record between our read and write: re-read, re-validate, retry
    raise HTTPException(409, "record was modified concurrently, please retry")


def plan_regularization(rec: dict, emp: dict, day: date, body: RegularizeIn) -> tuple[dict, dict]:
    """Validate the request against the current record and build (update, compare-and-set guard)."""
    old_status = rec["status"]
    old_in, old_out = as_utc(rec.get("punch_in")), as_utc(rec.get("punch_out"))
    sup_in = ms_to_dt(body.punch_in) if body.punch_in is not None else None
    sup_out = ms_to_dt(body.punch_out) if body.punch_out is not None else None
    new_status = body.status or old_status

    if new_status in NON_PRESENCE:
        if sup_in is not None or sup_out is not None:
            raise unprocessable(f"punch times cannot be supplied with status {new_status}")
        new_in = new_out = None
    else:
        new_in = sup_in or old_in
        new_out = sup_out or old_out
        if new_in is None:
            raise unprocessable(f"status {new_status} requires a punch_in", "punch_in")

    if sup_in is not None and attendance_date(sup_in, emp["shift_start"], emp["shift_end"]) != day:
        raise unprocessable("punch_in must stay on the record's attendance date", "punch_in")
    if new_out is not None and (sup_in is not None or sup_out is not None):
        if new_out <= new_in or (new_out - new_in).total_seconds() > MAX_SHIFT_SECONDS:
            raise unprocessable("punch_out must be after punch_in and within 24 hours of it", "punch_out")

    old = {
        "status": old_status,
        "punch_in": old_in,
        "punch_out": old_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day", False)),
    }
    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
           **derived_fields(new_status, new_in, new_out, day, emp)}
    changes = {k: {"from": old[k], "to": new[k]} for k in old if old[k] != new[k]}
    if not changes:
        raise unprocessable("the request changes nothing")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    n = len(rec.get("history") or [])
    guard: dict[str, Any] = {
        "_id": rec["_id"],
        # the record must still look the way we read it ...
        "status": old_status,
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        # ... and its history must still have exactly n entries (so no entry is ever lost or overwritten)
        f"history.{n}": {"$exists": False},
    }
    if n:
        guard[f"history.{n - 1}"] = {"$exists": True}
    return {"$set": new, "$push": {"history": entry}}, guard


# --------------------------------------------------------------------------- #
# 5. Analytics: aggregation-pipeline builders + endpoints
#
# Every analytics number is computed inside MongoDB from the STORED derived values.
# Python only validates input, does calendar arithmetic that is not data (working days), and applies the
# final half-up rounding with Decimal (Mongo's $round is banker's rounding, which R8 forbids).
# Sums are kept as integers (half-days, hundredths of an hour) so nothing is lost to float error.
# --------------------------------------------------------------------------- #
def is_presence(status_expr) -> dict:
    return {"$in": [status_expr, PRESENCE]}


def is_weekday(date_str_expr) -> dict:
    """date string 'YYYY-MM-DD' -> Mon-Fri?  ($dayOfWeek: 1 = Sunday ... 7 = Saturday)."""
    parsed = {"$dateFromString": {"dateString": date_str_expr, "format": "%Y-%m-%d", "onError": None, "onNull": None}}
    return {"$in": [{"$dayOfWeek": parsed}, [2, 3, 4, 5, 6]]}


def half_units(rec: str) -> dict:
    """Presence value of a record in HALF days: 1 for a half day, 2 otherwise. `rec` is a field/variable path."""
    return {"$cond": [{"$eq": [{"$ifNull": [f"{rec}.half_day", False]}, True]}, 1, 2]}


def late_flag(field: str) -> dict:
    return {"$cond": [{"$gt": [{"$ifNull": [field, 0]}, 0]}, 1, 0]}


def month_range(month: str) -> tuple[str, str]:
    first, last = parse_month(month)
    return first.isoformat(), last.isoformat()


# ---- employee monthly -------------------------------------------------------
def pipeline_employee_monthly(emp_code: str, month: str) -> list:
    start, end = month_range(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        {"$group": {
            "_id": None,
            # presence counts only on Mon-Fri (R7); weekend records still feed late / overtime totals
            "present_x2": {"$sum": {"$cond": [{"$and": [is_presence("$status"), is_weekday("$date")]}, half_units("$$ROOT"), 0]}},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": late_flag("$late_minutes")},
            "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$")):
    first, last = parse_month(month)
    emp = get_employee_or_404(emp_code)
    try:
        joined = date.fromisoformat(emp["joined_on"])
    except (ValueError, TypeError, KeyError):
        joined = first
    working_days = count_weekdays(max(first, joined), last)  # R7: counted from joined_on for mid-month joiners
    row = next(iter(logs.aggregate(pipeline_employee_monthly(emp_code, month))), None) or {}
    present_x2 = row.get("present_x2", 0)
    pct = None
    if working_days:
        pct = float((Decimal(present_x2) * 50 / Decimal(working_days)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_x2 / 2,
        "leave_days": row.get("leave_days", 0),
        "late_count": row.get("late_count", 0),
        "total_late_minutes": row.get("total_late_minutes", 0),
        "total_overtime_minutes": row.get("total_overtime_minutes", 0),
        "attendance_pct": pct,
    }


# ---- department summary -----------------------------------------------------
def pipeline_department_summary(month: str, department: Optional[str]) -> list:
    """Starts from `employees` (not from the logs) so people with zero logs are still counted (R9)."""
    start, end = month_range(month)
    match: dict[str, Any] = {"joined_on": {"$lte": end}}  # joined on/before the end of the period
    if department:
        match["department"] = department

    def per_log(expr: dict) -> dict:
        return {"$sum": {"$map": {"input": "$logs", "as": "l", "in": expr}}}

    present = is_presence("$$l.status")
    has_hours = {"$isNumber": "$$l.work_hours"}
    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": start, "$lte": end}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "work_hours": 1, "late_minutes": 1}},
            ],
            "as": "logs",
        }},
        # per employee first (an empty `logs` array simply sums to 0) ...
        {"$project": {
            "department": 1,
            "present_x2": per_log({"$cond": [{"$and": [present, is_weekday("$$l.date")]}, half_units("$$l"), 0]}),
            # work hours as integer hundredths so the department-wide mean is exact
            "hours_x100": per_log({"$cond": [{"$and": [present, has_hours]}, {"$round": [{"$multiply": ["$$l.work_hours", 100]}, 0]}, 0]}),
            "hours_n": per_log({"$cond": [{"$and": [present, has_hours]}, 1, 0]}),
            "late_count": per_log(late_flag("$$l.late_minutes")),
            "late_minutes": per_log({"$ifNull": ["$$l.late_minutes", 0]}),
            "leave_count": per_log({"$cond": [{"$eq": ["$$l.status", "LEAVE"]}, 1, 0]}),
            "on_duty_count": per_log({"$cond": [{"$eq": ["$$l.status", "ON_DUTY"]}, 1, 0]}),
        }},
        # ... then per department: one headcount tick per matching employee, with or without logs
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_x2": {"$sum": "$present_x2"},
            "hours_x100": {"$sum": "$hours_x100"},
            "hours_n": {"$sum": "$hours_n"},
            "late_count": {"$sum": "$late_count"},
            "total_late_minutes": {"$sum": "$late_minutes"},
            "leave_count": {"$sum": "$leave_count"},
            "on_duty_count": {"$sum": "$on_duty_count"},
        }},
        {"$sort": {"_id": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$"), department: Optional[str] = None):
    items = []
    for r in employees.aggregate(pipeline_department_summary(month, department)):
        avg = None
        if r["hours_n"]:  # mean over RECORDS, not an average of per-employee averages
            avg = float((Decimal(r["hours_x100"]) / Decimal(r["hours_n"]) / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        items.append({
            "department": r["_id"],
            "headcount": r["headcount"],
            "present_days": r["present_x2"] / 2,
            "avg_work_hours": avg,
            "late_count": r["late_count"],
            "total_late_minutes": r["total_late_minutes"],
            "leave_count": r["leave_count"],
            "on_duty_count": r["on_duty_count"],
        })
    return {"month": month, "items": items}


# ---- late leaderboard -------------------------------------------------------
def pipeline_late_leaderboard(month: str, limit: int, department: Optional[str]) -> list:
    start, end = month_range(month)
    pipeline: list[dict[str, Any]] = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees are ignored
    ]
    if department:  # rank INSIDE the department, so filter before ranking
        pipeline.append({"$match": {"emp.department": department}})
    pipeline += [
        # standard competition ranking (1, 2, 2, 4): $rank gives tied rows the same rank and skips the next ones
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit AFTER ranking: a tie at the cutoff keeps every tied row
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name", "department": "$emp.department",
                      "total_late_minutes": 1, "late_count": 1}},
    ]
    return pipeline


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    items = list(logs.aggregate(pipeline_late_leaderboard(month, limit, department)))
    return {"month": month, "items": items}


# ---- department trend -------------------------------------------------------
def pipeline_department_trend(department: str, d_from: date, d_to: date) -> list:
    """One row per calendar day, built in the database:
       employees(dept) -> their logs -> per-day totals -> day series from $range (gap filling) -> headcount per day
       -> integer-exact 4-decimal rate -> $setWindowFields moving average."""
    n_days = (d_to - d_from).days + 1
    origin = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    f, t = d_from.isoformat(), d_to.isoformat()
    return [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": f, "$lte": t}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
            ],
            "as": "l",
        }},
        {"$unwind": {"path": "$l", "preserveNullAndEmptyArrays": True}},  # keep employees with no logs: the stream never ends up empty
        {"$group": {
            "_id": "$l.date",
            "present_x2": {"$sum": {"$cond": [is_presence("$l.status"), half_units("$l"), 0]}},  # any weekday or weekend
            "late": {"$sum": late_flag("$l.late_minutes")},
        }},
        {"$group": {"_id": None, "rows": {"$push": {"d": "$_id", "present_x2": "$present_x2", "late": "$late"}}}},
        # gap filling: one element per requested day (weekends and empty days included), joined to the real rows
        {"$project": {"_id": 0, "days": {"$map": {
            "input": {"$range": [0, n_days]}, "as": "i",
            "in": {"$let": {
                "vars": {"dt": {"$add": [origin, {"$multiply": ["$$i", 86400000]}]}},
                "in": {"$let": {
                    "vars": {"ds": {"$dateToString": {"format": "%Y-%m-%d", "date": "$$dt"}}},
                    "in": {
                        "date": "$$ds",
                        "dow": {"$dayOfWeek": "$$dt"},
                        "row": {"$arrayElemAt": [{"$filter": {"input": "$rows", "as": "r", "cond": {"$eq": ["$$r.d", "$$ds"]}}}, 0]},
                    },
                }},
            }},
        }}}},
        {"$unwind": "$days"},
        {"$project": {
            "date": "$days.date",
            "is_working": {"$in": ["$days.dow", [2, 3, 4, 5, 6]]},
            "present_x2": {"$ifNull": ["$days.row.present_x2", 0]},
            "late": {"$ifNull": ["$days.row.late", 0]},
        }},
        # R9 headcount per day: department employees with joined_on <= that day (indexed lookup, <= 92 of them)
        {"$lookup": {
            "from": "employees", "let": {"d": "$date"},
            "pipeline": [{"$match": {"department": department, "$expr": {"$lte": ["$joined_on", "$$d"]}}}, {"$count": "n"}],
            "as": "hc",
        }},
        {"$addFields": {"headcount": {"$ifNull": [{"$arrayElemAt": ["$hc.n", 0]}, 0]}}},
        # rate in 1/10000ths, rounded HALF-UP with integer arithmetic: floor((2*p*10000/2h) + 1/2)
        {"$addFields": {"rate_k": {"$cond": [
            {"$and": ["$is_working", {"$gt": ["$headcount", 0]}]},
            {"$floor": {"$divide": [{"$add": [{"$multiply": ["$present_x2", 10000]}, "$headcount"]}, {"$multiply": [2, "$headcount"]}]}},
            None,
        ]}}},
        {"$setWindowFields": {"sortBy": {"date": 1}, "output": {
            "sum_k": {"$sum": "$rate_k", "window": {"documents": [-6, 0]}},
            "n_k": {"$sum": {"$cond": [{"$ne": ["$rate_k", None]}, 1, 0]}, "window": {"documents": [-6, 0]}},
        }}},
        {"$project": {
            "_id": 0,
            "date": 1,
            "is_working_day": "$is_working",
            "headcount": 1,
            "present_count": {"$divide": ["$present_x2", 2]},
            "late_count": "$late",
            "attendance_rate": {"$cond": [{"$eq": ["$rate_k", None]}, None, {"$divide": ["$rate_k", 10000]}]},
            # mean of the (reported, 4-decimal) rates in the window, half-up to 4 decimals
            "moving_avg_7d": {"$cond": [
                {"$gt": ["$n_k", 0]},
                {"$divide": [{"$floor": {"$divide": [{"$add": [{"$multiply": ["$sum_k", 2]}, "$n_k"]}, {"$multiply": [2, "$n_k"]}]}}, 10000]},
                None,
            ]},
        }},
        {"$sort": {"date": 1}},
    ]


def validated_trend_range(d_from: str, d_to: str) -> tuple[date, date]:
    lo, hi = parse_date(d_from, "from"), parse_date(d_to, "to")
    if hi < lo:
        raise unprocessable("'to' must not be before 'from'", "to")
    if (hi - lo).days + 1 > MAX_TREND_DAYS:
        raise unprocessable(f"range must not exceed {MAX_TREND_DAYS} days", "to")
    return lo, hi


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(..., alias="from"), to: str = Query(...)):
    lo, hi = validated_trend_range(from_, to)
    if employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(404, "unknown department")
    return {"department": department, "items": list(employees.aggregate(pipeline_department_trend(department, lo, hi)))}


# --------------------------------------------------------------------------- #
# 6. Explain: runs the SAME query / pipeline builders as the real endpoints
# --------------------------------------------------------------------------- #
def _require(**params):
    missing = [k for k, v in params.items() if v in (None, "")]
    if missing:
        raise unprocessable("missing required query parameter(s): " + ", ".join(missing), "query")


def _explain(command: dict) -> dict:
    raw = db.command("explain", command, verbosity="executionStats")
    return json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS))  # ObjectId/Timestamp-safe


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"] = Path(...),
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[ALL_STATUSES] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    if endpoint == "attendance_list":
        q = attendance_filter(emp_code, date_from, date_to, status)
        cmd = {"find": logs.name, "filter": q, "sort": dict(ATTENDANCE_SORT),
               "skip": (page - 1) * page_size, "limit": page_size}
        coll = logs.name
    else:
        if endpoint == "employee_monthly":
            _require(emp_code=emp_code, month=month)
            pipeline, coll = pipeline_employee_monthly(emp_code, month), logs.name
        elif endpoint == "department_summary":
            _require(month=month)
            pipeline, coll = pipeline_department_summary(month, department), employees.name
        elif endpoint == "late_leaderboard":
            _require(month=month)
            pipeline, coll = pipeline_late_leaderboard(month, limit, department), logs.name
        else:
            _require(department=department, **{"from": from_, "to": to})
            lo, hi = validated_trend_range(from_, to)
            pipeline, coll = pipeline_department_trend(department, lo, hi), employees.name
        cmd = {"aggregate": coll, "pipeline": pipeline, "cursor": {}}
    return {"endpoint": endpoint, "collection": coll, "explain": _explain(cmd)}
