# DECISIONS.md

1. **Indexes.** On attendance_logs: unique (emp_code, date) guarantees one record per day and serves per-employee queries; (date desc, emp_code) the default list sort; (status, date, emp_code) the status filter; (date, late_minutes, emp_code) the leaderboard; (emp_code, punch_in desc) punch-out. On employees: unique emp_code, (department, joined_on) for headcount, (department, emp_code) for listing. I rejected copying department onto every log, because the seeded data lacks it.

2. **Punch-in race.** Both requests validate and compute the same date, then both insert. The unique index lets the database accept one; the other gets a duplicate-key error, which becomes 409. There is no check-first step to slip between.

3. **Ties.** I rank first (`$rank`: 1, 2, 2, 4), then keep `rank <= limit`. A tie at the cutoff returns every tied employee, so the list can exceed `limit`. Ties are ordered by emp_code.

4. **Headcount.** The pipeline starts from employees who joined by month end and looks up their logs. An empty log array sums to zero, so people with no logs still count once.

5. **100x data.** Keep daily rollups per employee and department, updated on every punch and correction, and serve analytics from those instead of raw logs.
