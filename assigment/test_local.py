"""Local tests (mongomock). Covers rules R1-R5, validation, punch/patch flows, concurrency guards.
Aggregation pipelines that need $setWindowFields/$densify cannot run on mongomock - see test_pipelines.py."""
import sys, threading
from datetime import datetime, timezone, timedelta
import mongomock, pytest
sys.path.insert(0, "/home/claude/candidate_kit")
import app.main as m
from fastapi.testclient import TestClient

IST = m.IST
def ms(y, mo, d, h, mi, s=0, ms_=0):
    return int(datetime(y, mo, d, h, mi, s, tzinfo=IST).timestamp()) * 1000 + ms_

@pytest.fixture()
def api():
    c = mongomock.MongoClient(tz_aware=True)
    m.client, m.db = c, c["t"]
    m.employees, m.logs = m.db["employees"], m.db["attendance_logs"]
    m.ensure_indexes()
    return TestClient(m.app, raise_server_exceptions=False)

def emp(api, code="EMP0001", **kw):
    body = dict(emp_code=code, name="A", email="a@x.com", department="Eng", joined_on="2026-01-05"); body.update(kw)
    return api.post("/employees", json=body)

# ---------------- pure rules ----------------
def D(*a): return datetime(*a, tzinfo=IST).astimezone(timezone.utc)
def test_late_rule_r2():
    day = datetime(2026,7,6).date()
    f = lambda h,mi,s: m.compute_late_minutes(D(2026,7,6,h,mi,s), day, "09:30", "18:30")
    assert f(9,40,0) == 0 and f(9,40,1) == 10 and f(10,15,59) == 45 and f(9,0,0) == 0
def test_overtime_r3():
    day = datetime(2026,7,6).date()
    f = lambda h,mi,s: m.compute_overtime(D(2026,7,6,h,mi,s), day, "09:30", "18:30")
    assert f(18,59,59) == 0 and f(19,0,0) == 30 and f(19,10,0) == 40 and f(17,0,0) == 0
def test_overnight_overtime_and_date():
    assert m.attendance_date(D(2026,7,6,21,55), "22:00", "06:00").isoformat() == "2026-07-06"
    assert m.attendance_date(D(2026,7,7,3,0), "22:00", "06:00").isoformat() == "2026-07-06"   # earlier than shift_end
    assert m.attendance_date(D(2026,7,7,6,0), "22:00", "06:00").isoformat() == "2026-07-07"   # not strictly earlier
    day = datetime(2026,7,6).date()
    assert m.compute_overtime(D(2026,7,7,6,40), day, "22:00", "06:00") == 40
def test_work_hours_half_up():
    a = datetime(2026,7,6,4,0,tzinfo=timezone.utc)
    assert m.compute_work_hours(a, a+timedelta(seconds=32580)) == 9.05      # 9.05 exactly
    assert m.compute_work_hours(a, a+timedelta(seconds=18)) == 0.01         # 0.005 -> half-up
    assert m.compute_work_hours(a, a+timedelta(hours=9, minutes=7)) == 9.12
    assert m.compute_half_day(4.49) and not m.compute_half_day(4.5)

# ---------------- employees ----------------
def test_employee_crud(api):
    r = emp(api); assert r.status_code == 201 and isinstance(r.json()["created_at"], int) and "_id" not in r.json()
    assert emp(api).status_code == 409
    for bad in [dict(emp_code="E1"), dict(email="nope"), dict(shift_start="9:30"), dict(shift_end="09:30", shift_start="09:30"),
                dict(joined_on="2026-02-30"), dict(joined_on="20260105"), dict(name=""), dict(department="")]:
        assert emp(api, "EMP0009", **bad).status_code == 422, bad
    for i in range(2, 8): emp(api, f"EMP000{i}", department="Ops" if i % 2 else "Eng")
    r = api.get("/employees", params=dict(page_size=2, page=2)).json()
    assert [e["emp_code"] for e in r["items"]] == ["EMP0003", "EMP0004"] and r["total"] == 7
    r = api.get("/employees", params=dict(department="Ops")).json(); assert r["total"] == 3
    for q in [dict(page=0), dict(page_size=101), dict(page_size=0)]:
        assert api.get("/employees", params=q).status_code == 422

# ---------------- punch-in ----------------
def test_punch_in(api):
    assert api.post("/attendance/punch-in", json=dict(emp_code="EMP0404")).status_code == 404
    emp(api)
    t = ms(2026,7,6,10,5,0,900)
    r = api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", punched_at=t)); j = r.json()
    assert r.status_code == 201 and j["date"] == "2026-07-06" and j["late_minutes"] == 35
    assert j["punch_in"] == t - 900 and j["punch_out"] is None and j["work_hours"] is None and j["history"] == [] and "id" not in j
    assert api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", punched_at=t)).status_code == 409
    for bad in [dict(punched_at=1783312500), dict(punched_at=1783312500000.5), dict(punched_at="1783312500000"),
                dict(punched_at=True), dict(punched_at=99999999999), dict(status="ABSENT"), dict(status="x")]:
        assert api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", **bad)).status_code == 422, bad
    # derived fields sent by client are ignored
    r = api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,7,9,0), late_minutes=99, work_hours=3))
    assert r.status_code == 201 and r.json()["late_minutes"] == 0

def test_punch_in_race(api):
    emp(api); out = []
    def go(): out.append(api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,9,30))).status_code)
    ts = [threading.Thread(target=go) for _ in range(8)]; [t.start() for t in ts]; [t.join() for t in ts]
    assert sorted(out) == [201] + [409] * 7

# ---------------- punch-out ----------------
def test_punch_out(api):
    emp(api)
    assert api.post("/attendance/punch-out", json=dict(emp_code="EMP0404")).status_code == 404
    assert api.post("/attendance/punch-out", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,19,0))).status_code == 404
    api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,10,5)))
    for t in (ms(2026,7,6,10,5), ms(2026,7,7,10,5,1)):  # == punch_in (422) / 24h+1s later (422)
        pass
    assert api.post("/attendance/punch-out", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,10,5))).status_code == 422
    assert api.post("/attendance/punch-out", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,7,10,5,1))).status_code == 422
    r = api.post("/attendance/punch-out", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,19,10)))
    j = r.json(); assert r.status_code == 200 and j["overtime_minutes"] == 40 and j["work_hours"] == 9.08 and j["half_day"] is False and j["late_minutes"] == 35
    assert api.post("/attendance/punch-out", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,19,20))).status_code == 409

def test_punch_out_overnight(api):
    emp(api, "EMP0005", shift_start="22:00", shift_end="06:00")
    j = api.post("/attendance/punch-in", json=dict(emp_code="EMP0005", punched_at=ms(2026,7,6,21,55))).json()
    assert j["date"] == "2026-07-06" and j["late_minutes"] == 0
    j = api.post("/attendance/punch-out", json=dict(emp_code="EMP0005", punched_at=ms(2026,7,7,6,40))).json()
    assert j["work_hours"] == 8.75 and j["overtime_minutes"] == 40 and j["date"] == "2026-07-06"

def test_punch_out_race(api):
    emp(api); api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,9,30))); out = []
    def go(): out.append(api.post("/attendance/punch-out", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,18,30))).status_code)
    ts = [threading.Thread(target=go) for _ in range(8)]; [t.start() for t in ts]; [t.join() for t in ts]
    assert sorted(out) == [200] + [409] * 7

def test_half_day(api):
    emp(api); api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,9,30)))
    j = api.post("/attendance/punch-out", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,6,13,59))).json()
    assert j["work_hours"] == 4.48 and j["half_day"] is True

# ---------------- list attendance ----------------
def test_list_attendance(api):
    emp(api); emp(api, "EMP0002")
    for code in ("EMP0002", "EMP0001"):
        for d in (6, 7, 8):
            api.post("/attendance/punch-in", json=dict(emp_code=code, punched_at=ms(2026,7,d,9,30)))
    r = api.get("/attendance", params=dict(page_size=4)).json()
    assert [(i["date"], i["emp_code"]) for i in r["items"]] == [("2026-07-08","EMP0001"),("2026-07-08","EMP0002"),("2026-07-07","EMP0001"),("2026-07-07","EMP0002")]
    assert r["total"] == 6
    r = api.get("/attendance", params=dict(emp_code="EMP0001", date_from="2026-07-07", date_to="2026-07-07")).json(); assert r["total"] == 1
    assert api.get("/attendance", params=dict(date_from="2026-07-09", date_to="2026-07-01")).status_code == 422
    assert api.get("/attendance", params=dict(date_from="2026-13-01")).status_code == 422
    assert api.get("/attendance", params=dict(status="BOGUS")).status_code == 422
    # legacy doc without half_day / history
    m.logs.insert_one(dict(emp_code="EMP0002", date="2026-07-14", status="PRESENT", punch_in=datetime(2026,7,14,4,2,tzinfo=timezone.utc),
                           punch_out=datetime(2026,7,14,13,1,tzinfo=timezone.utc), work_hours=8.98, late_minutes=0, overtime_minutes=0))
    it = api.get("/attendance", params=dict(date_from="2026-07-14")).json()["items"][0]
    assert it["half_day"] is False and it["history"] == [] and isinstance(it["punch_in"], int) and "id" not in it

# ---------------- PATCH / regularize ----------------
def seed_day(api):
    emp(api); api.post("/attendance/punch-in", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,7,10,5)))
    api.post("/attendance/punch-out", json=dict(emp_code="EMP0001", punched_at=ms(2026,7,7,19,0)))
def patch(api, **body):
    body.setdefault("reason", "biometric glitch"); body.setdefault("regularized_by", "hr.admin")
    return api.patch("/attendance/EMP0001/2026-07-07", json=body)

def test_regularize_basic(api):
    seed_day(api)
    r = patch(api, punch_in=ms(2026,7,7,9,28)); j = r.json()
    assert r.status_code == 200 and j["late_minutes"] == 0 and j["work_hours"] == 9.53 and len(j["history"]) == 1
    h = j["history"][0]; assert h["by"] == "hr.admin" and h["changes"]["punch_in"] == dict(**{"from": ms(2026,7,7,10,5)}, to=ms(2026,7,7,9,28))
    assert h["changes"]["late_minutes"] == {"from": 35, "to": 0} and "overtime_minutes" not in h["changes"] and "half_day" not in h["changes"]
    assert patch(api, punch_in=ms(2026,7,7,9,28)).status_code == 422          # changes nothing
    j = patch(api, status="LEAVE").json(); assert j["punch_in"] is None and j["work_hours"] is None and len(j["history"]) == 2
    assert j["history"][1]["changes"]["status"] == {"from": "PRESENT", "to": "LEAVE"}
    assert patch(api, status="ABSENT", punch_in=ms(2026,7,7,9,0)).status_code == 422
    assert patch(api, status="PRESENT").status_code == 422                    # presence needs a punch_in
    assert patch(api, status="WFH", punch_in=ms(2026,7,7,9,0)).status_code == 200

def test_regularize_errors(api):
    seed_day(api)
    assert patch(api, punch_in=ms(2026,7,8,9,0)).status_code == 422           # off the attendance date
    assert patch(api, punch_out=ms(2026,7,7,9,0)).status_code == 422          # before punch_in
    assert patch(api, punch_out=ms(2026,7,8,10,6)).status_code == 422          # > 24h
    assert patch(api, reason="abc").status_code == 422
    assert patch(api, punch_in=None).status_code == 422
    assert api.patch("/attendance/EMP0001/2026-07-07", json=dict(status="LEAVE")).status_code == 422
    assert api.patch("/attendance/EMP0404/2026-07-07", json=dict(status="LEAVE", reason="12345", regularized_by="x")).status_code == 404
    assert api.patch("/attendance/EMP0001/2026-07-09", json=dict(status="LEAVE", reason="12345", regularized_by="x")).status_code == 404
    assert api.patch("/attendance/EMP0001/2026-02-30", json=dict(status="LEAVE", reason="12345", regularized_by="x")).status_code == 422
    # a client-sent derived field is ignored, not applied
    j = patch(api, punch_in=ms(2026,7,7,9,28), work_hours=1.0, late_minutes=77).json(); assert j["work_hours"] == 9.53 and j["late_minutes"] == 0

def test_regularize_concurrent_keeps_all_history(api):
    seed_day(api); out = []
    def go(i): out.append(patch(api, punch_in=ms(2026,7,7,9,10+i), reason=f"edit number {i}").status_code)
    ts = [threading.Thread(target=go, args=(i,)) for i in range(5)]; [t.start() for t in ts]; [t.join() for t in ts]
    ok = out.count(200); hist = api.get("/attendance", params=dict(emp_code="EMP0001")).json()["items"][0]["history"]
    assert ok >= 1 and set(out) <= {200, 409, 422} and len(hist) == ok and len({h["reason"] for h in hist}) == ok

def test_health_and_explain_validation(api):
    assert api.get("/health").status_code == 200
    assert api.get("/admin/explain/employee_monthly").status_code == 422
    assert api.get("/admin/explain/department_trend", params=dict(department="Eng")).status_code == 422
    assert api.get("/admin/explain/nope").status_code == 422
