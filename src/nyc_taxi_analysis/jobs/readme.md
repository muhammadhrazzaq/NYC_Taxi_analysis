# Data Quality Rules
 
How raw trips become trusted data, and the evidence behind each decision.
 
Every rule here was derived by **profiling bronze first** (in Athena), not assumed up front. Where the data contradicted a "standard" cleaning rule, the data won. The cross-file trips are the clearest example.
 
---
 
## 1. Principles
 
1. **Nothing disappears without trace.** Every bronze row ends up in exactly one place: clean, quarantined, fare adjustment, or removed duplicate.
2. **Invalid is not the same as unusual.** Impossible values are quarantined; unusual-but-possible values are kept and **flagged**.
3. **Different-shaped valid data gets its own table.** Refunds and reversals aren't errors, so they go to `fare_adjustments`.
4. **One reason per row.** Rules are evaluated in a fixed order and the first failure wins, so counts per reason add up.
5. **Reconcile every run.** The silver job fails (and stops the pipeline) if the totals don't balance.
---
 
## 2. Bronze rules (ingestion)
 
| Rule | Action |
|---|---|
| Explicit 18-column schema; no `inferSchema` | Enforced at read |
| Row fails to parse against the schema (`PERMISSIVE` mode) | Raw line kept in `_corrupt` → `bronze.trips_quarantine` |
| `year` and `month` taken from the **file name**, not the pickup date | Avoids junk partitions from bad clocks (e.g. 2009, 2088) |
| Lineage columns: `source_file`, `ingested_at` | Added to every row |
| Writes use `overwritePartitions()` | Rerunning a month replaces it; verified by an unchanged count after rerun |
 
---
 
## 3. Silver quarantine rules
 
Evaluated **in this order**; the first match sets `dq_reason`.
 
| # | `dq_reason` | Condition | Evidence (bronze profiling) | Why |
|---|---|---|---|---|
| 1 | `non_positive_duration` | drop-off ≤ pickup | 89,309 rows | Physically impossible |
| 2 | `duration_over_6h` | duration > 360 min | 252,185 rows | Meter left running or clock error |
| 3 | `pickup_out_of_range` | see §4 | ~2.5k rows | Clock errors and dates outside the dataset |
| 4 | `invalid_distance` | distance < 0 or > 200 miles | 11,440 negative · 111 over 200 | Impossible or GPS/meter error |
| 5 | `total_over_1000` | total > $1,000 | 96 rows (max fare $943,274.80) | Input error; p99 fare is $53.62 |
 
> Counts are from independent profiling queries. Because a row is labelled with its *first* failing rule only, the per-reason counts in `silver.trips_quarantine` can be slightly lower than these.
 
---
 
## 4. Pickup-month rule (cross-file trips)
 
`month_offset = (pickup year × 12 + pickup month) − (file year × 12 + file month)`
 
Profiling the 745,830 rows whose pickup month differed from their file month showed:
 
| Offset | Rows | Pattern | Decision |
|---|---:|---|---|
| **+1** | 743,223 | **742,589 (99.9%) on day 1** of the next month; **0** also present in the next month's file | **Keep** day-1 rows, flag `is_cross_file`; quarantine the 634 on days 2–31 |
| −1 | 743 | Boundary trips | **Keep**, flag `is_cross_file` |
| Beyond ±1 | 1,864 | Scattered from −221 to +852 months (2009, 2088…) | **Quarantine** |
 
Also quarantined: pickups **before 2019-01-01** or **on/after 2020-07-01** (outside the dataset range).
 
**Rule:**
```text
out_of_range =
      month_offset NOT IN (-1, 0, 1)
   OR (month_offset = 1 AND day(pickup) != 1)
   OR pickup <  2019-01-01
   OR pickup >= 2020-07-01
```
 
**Effect:** silver is partitioned by the **actual pickup month**, so its monthly counts differ from bronze's. This is expected; the totals still reconcile.
 
---
 
## 5. Fare adjustments
 
Rows that pass §3 but have `fare_amount < 0` **or** `total_amount < 0` go to `silver.fare_adjustments`.
 
| Payment type | Meaning | Negative-fare rows | `adjustment_reason` |
|---|---|---:|---|
| 4 | Dispute | 97,445 | `dispute_or_no_charge` |
| 3 | No charge | 96,467 | `dispute_or_no_charge` |
| 2 | Cash | 31,128 | `negative_fare_other` |
| null | Unknown | 517 | `negative_fare_other` |
| 1 | Credit card | 182 | `negative_fare_other` |
 
About 86% are dispute or no-charge reversals of an earlier trip. Keeping them separate stops them distorting trip metrics while still allowing net-revenue and dispute-rate analysis. Cash reversals are recorded as unexplained and kept for analysis.
 
---
 
## 6. Deduplication
 
Exact duplicates on the business key are removed, and one copy kept:
 
```text
VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
trip_distance, PULocationID, DOLocationID, payment_type,
fare_amount, tip_amount, total_amount
```
 
Evidence: 12,957 duplicate groups accounting for **13,000** extra rows.
 
---
 
## 7. Flags (kept rows)
 
| Flag | Condition | Evidence |
|---|---|---|
| `is_cross_file` | `month_offset != 0` | ~743k rows (§4) |
| `is_zero_distance` | `trip_distance = 0` | 935,450 rows; often cancellations or waiting time, and the fare may still be real |
| `is_missing_meter_info` | `VendorID`, `RatecodeID` and `payment_type` all null | 527,484 rows; mostly 2020, fares, times and zones still present |
| `is_passenger_count_suspect` | passenger count null, 0, or > 6 | 0: 1,856,608 · 7–9: 1,065 · null: ~527k |
 
---
 
## 8. Derived columns (silver)
 
| Column | Definition |
|---|---|
| `trip_duration_min` | (drop-off − pickup) in minutes |
| `surcharges_amount` | `extra + mta_tax + improvement_surcharge + congestion_surcharge` (nulls as 0) |
| `pickup_year`, `pickup_month` | From the pickup timestamp; partition columns |
| `source_year`, `source_month` | From the file name (bronze `year` and `month`, renamed) |
 
---
 
## 9. Reconciliation gate
 
Enforced at the end of every silver run:
 
```text
bronze rows = silver.trips + silver.trips_quarantine + silver.fare_adjustments + duplicates removed
```
 
If it doesn't hold, the job raises an error, Airflow marks downstream tasks `upstream_failed`, and gold keeps its last good version.
 
> This gate caught a real bug: a typo (`isin(-1, 1)` instead of `isin(-1, 0, 1)`) quarantined 100.9M rows.
 
---
 
## 10. Gold rules
 
| Rule | Detail |
|---|---|
| Row count | `gold.obt_trips` must equal `silver.trips` exactly (left joins only; a mismatch means a join duplicated or dropped rows) |
| Zone joins | Broadcast join to `bronze.zones`, twice (pickup `pu_*`, drop-off `do_*`); verified as `BroadcastHashJoin` in `explain()` |
| Unknown zones | IDs 264/265 ("Unknown"/"N/A" in the lookup) flagged `is_unknown_zone` |
| Code decoding | Unknown codes become `"Unknown"`, never null |
 
| Code | Decoding |
|---|---|
| `VendorID` | 1 Creative Mobile Technologies · 2 VeriFone · 4 Unknown (4) |
| `payment_type` | 1 Credit card · 2 Cash · 3 No charge · 4 Dispute · 5 Unknown · 6 Voided trip |
| `RatecodeID` | 1 Standard rate · 2 JFK · 3 Newark · 4 Nassau or Westchester · 5 Negotiated fare · 6 Group ride · 99 Unknown |
 
| Measure | Rule |
|---|---|
| `tip_pct` | Card payments only (cash tips aren't recorded), and only when `fare_amount > 0` |
| Tip aggregates | Limited to 0–100% to stop outliers skewing averages; the median is reported alongside |
| `avg_speed_mph` | Only when duration > 0 |
 
---
 
## 11. Timestamps
 
TLC timestamps are **NYC local wall-clock times**. All Spark jobs run with `spark.sql.session.timeZone = UTC`, so the values are never converted. A previous `America/New_York` session setting shifted `pickup_hour` by 4–5 hours and moved trips across midnight and month boundaries.
 
Check: the hourly distribution should bottom out around **4–5am** and peak around **6–7pm**.
 