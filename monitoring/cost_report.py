"""Daily cost report: what every service cost, from measured usage x list prices.

    uv run --env-file .env python monitoring/cost_report.py            # yesterday (UTC)
    uv run --env-file .env python monitoring/cost_report.py --day 2026-10-21
    ... --email     # also send it (SMTP_USER / SMTP_PASSWORD / REPORT_TO)
    ... --save      # also write the day's rows to BigQuery monitoring.cost_daily

Google's billing data lags by hours and the trial credit hides it, so this measures usage
directly: BigQuery bytes from INFORMATION_SCHEMA.JOBS, Gemini tokens from
vectors.article_embeddings, Cloud Run job seconds from the executions API, storage sizes,
and OpenAI's own billed cost (Costs API, admin key in Secret Manager `openai-admin-key`).
Monthly free tiers are applied in date order, so a day is charged only once the month's
free allowance is used up. Read-only apart from --save; ~0.1 GB of BigQuery per run.

Not counted (inside free tiers at our volume): Workflows, Scheduler, Secret Manager,
Cloud Build, Logging, Monitoring. Not counted at all: Gemini query embeddings (~10 tokens
a question) and one-off eval embeddings (e.g. the $1.23 chunk pilot); the billing
export shows those: its official cost + credits for the previous day are a note at the end.
"""

from __future__ import annotations

import argparse
import base64
import calendar
import html
import os
import smtplib
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery

REGION = "asia-southeast1"
JOBS = ["guardian-ingestion", "vn-ingestion", "dbt-build", "embed-sync"]
CAP = 300_000_000
GIB, TIB = 2**30, 2**40

# USD list prices for asia-southeast1, from the Cloud Billing Catalog API (2026-10-23).
# (free monthly allowance, price per unit beyond it)
PRICES = {
    "bq_analysis_tib": (1.0, 8.4375),
    "bq_active_logical_gib_mo": (10.0, 0.02),
    "bq_longterm_logical_gib_mo": (10.0, 0.01),
    # Physical SKUs have their own 10 GiB; active price read from the billing export's
    # `price` field (2026-10-27), long-term assumed half of it like the logical SKUs.
    "bq_active_physical_gib_mo": (10.0, 0.046),
    "bq_longterm_physical_gib_mo": (10.0, 0.023),
    "run_jobs_vcpu_s": (0.0, 0.0000216),   # no free tier listed for jobs: conservative
    "run_jobs_gib_s": (0.0, 0.0000024),
    "gemini_embedding_2_mtok": (0.0, 0.20),
    "ar_storage_gib_mo": (0.5, 0.10),
}
GCS_GIB_MO = {("ASIA-SOUTHEAST1", "STANDARD"): 0.02, ("ASIA-SOUTHEAST1", "COLDLINE"): 0.005,
              ("US", "STANDARD"): 0.026}
# Pinecone Starter: 2 GB per project. Bytes per vector = 4 x dims + metadata; 682 B of
# metadata + snippet is from the 2026-10-24 measurement (1.35 GB / 494k vectors at 512 dims).
PINECONE_LIMIT_BYTES = 2e9
PINECONE_META_BYTES = 682


def tiered(key: str, used_before: float, used_day: float) -> float:
    """Cost of `used_day` units when `used_before` units were already used this month."""
    free, price = PRICES[key]
    return (max(0.0, used_before + used_day - free) - max(0.0, used_before - free)) * price


def _usage(r: dict) -> str:
    return f"{r['usage']:,} {r['unit']}" if r["unit"] else ""


class Report:
    def __init__(self, day: date, project: str):
        self.day, self.project = day, project
        self.month_start = day.replace(day=1)
        self.rows: list[dict] = []   # service, item, usage, unit, day_usd, mtd_usd
        self.notes: list[str] = []
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.http = AuthorizedSession(creds)
        self.bq = bigquery.Client(project=project, location=REGION)

    def add(self, service, item, usage, unit, day_usd, mtd_usd):
        self.rows.append(dict(service=service, item=item, usage=usage, unit=unit,
                              day_usd=round(day_usd, 4), mtd_usd=round(mtd_usd, 4)))

    def q(self, sql: str, params: list | None = None) -> list[dict]:
        cfg = bigquery.QueryJobConfig(maximum_bytes_billed=CAP, query_parameters=params or [],
                                      labels={"app": "cost-report"})
        return [dict(r.items()) for r in self.bq.query(sql, job_config=cfg).result()]

    def get(self, url: str, **params) -> dict:
        try:
            r = self.http.get(url, params=params, timeout=30)
        except OSError:  # a read timeout on the Run API hit 3 of 22 backfill days; retry once
            r = self.http.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def dparams(self):
        return [bigquery.ScalarQueryParameter("m0", "DATE", self.month_start),
                bigquery.ScalarQueryParameter("d", "DATE", self.day)]

    # ---------------------------------------------------------------- BigQuery queries
    def bigquery_queries(self):
        rows = self.q(f"""
            SELECT CASE WHEN user_email LIKE 'dbt-runner@%' THEN 'pipeline (Cloud Run jobs)'
                        WHEN user_email LIKE 'ci-runner@%' THEN 'PR CI'
                        WHEN user_email LIKE 'cost-reporter@%' THEN 'cost report'
                        WHEN user_email LIKE '%.gserviceaccount.com' THEN 'other service account'
                        ELSE 'local (agent / evals / dev)' END AS who,
                   DATE(creation_time) = @d AS is_day, SUM(total_bytes_billed) AS b
            FROM `region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE DATE(creation_time) BETWEEN @m0 AND @d AND job_type = 'QUERY'
              -- A script's bytes are the sum of its child statements, which are listed too
              -- (dbt's insert_overwrite MERGE): counting both overstated usage by ~40%.
              -- Without the parents it equals the billing export to 0.01 GiB (10-24..26).
              AND IFNULL(statement_type, '') != 'SCRIPT'
            GROUP BY 1, 2""", self.dparams())
        before = sum((r["b"] or 0) for r in rows if not r["is_day"]) / TIB
        day_total = sum((r["b"] or 0) for r in rows if r["is_day"]) / TIB
        day_cost = tiered("bq_analysis_tib", before, day_total)
        mtd_cost = tiered("bq_analysis_tib", 0, before + day_total)
        for who in sorted({r["who"] for r in rows}):
            d = sum((r["b"] or 0) for r in rows if r["who"] == who and r["is_day"]) / TIB
            m = sum((r["b"] or 0) for r in rows if r["who"] == who) / TIB
            share_d = d / day_total if day_total else 0
            share_m = m / (before + day_total) if before + day_total else 0
            self.add("BigQuery", f"queries: {who}", round(d * 1024, 2), "GiB scanned",
                     day_cost * share_d, mtd_cost * share_m)
        self.notes.append(f"BigQuery free queries: {(before + day_total) * 1024:,.0f} of 1,024 GiB "
                          f"used this month ({(before + day_total) * 100:.0f}%).")

    # ---------------------------------------------------------------- BigQuery storage
    def bigquery_storage(self):
        # Table metadata, not INFORMATION_SCHEMA.TABLE_STORAGE (needs a project-level grant).
        # Each dataset is billed on logical OR physical bytes. Physical bytes include time
        # travel (7 days of overwritten partitions: insert_overwrite every 4 h makes it about
        # half), and fail-safe (7 more days) is billed too but not in the metadata; with
        # steady churn it is about the time-travel size, so that stands in for it (10-26:
        # 29.1 GiB estimated vs 30.9 billed; logical-only had shown a fifth of the cost).
        la = ll = pa = pl = tt = 0.0
        for ds in self.bq.list_datasets():
            physical = self.bq.get_dataset(ds.reference)._properties.get("storageBillingModel") == "PHYSICAL"
            for t in self.bq.list_tables(ds.reference):
                if t.table_type != "TABLE":
                    continue
                props = self.bq.get_table(t.reference)._properties
                n = lambda k: int(props.get(k, 0)) / GIB  # noqa: E731
                if physical:
                    tt += n("numTimeTravelPhysicalBytes")
                    pa += n("numActivePhysicalBytes") + n("numTimeTravelPhysicalBytes")
                    pl += n("numLongTermPhysicalBytes")
                else:
                    la += n("numBytes") - n("numLongTermBytes")
                    ll += n("numLongTermBytes")
        days = calendar.monthrange(self.day.year, self.day.month)[1]
        lmo = tiered("bq_active_logical_gib_mo", 0, la) + tiered("bq_longterm_logical_gib_mo", 0, ll)
        pmo = tiered("bq_active_physical_gib_mo", 0, pa) + tiered("bq_longterm_physical_gib_mo", 0, pl)
        self.add("BigQuery", "storage (logical-billed datasets)", round(la + ll, 2), "GiB stored",
                 lmo / days, lmo / days * self.day.day)
        self.add("BigQuery", "storage (physical-billed, incl. time travel + fail-safe)", round(pa + pl, 2),
                 "GiB stored", pmo / days, pmo / days * self.day.day)
        if pa + pl:
            self.notes.append(f"BigQuery physical storage: {2 * tt:,.1f} of {pa + pl:,.1f} GiB is time travel "
                              f"+ fail-safe (overwritten partitions kept 7 + 7 days).")

    # ---------------------------------------------------------------- Gemini embeddings
    def gemini(self):
        r = self.q(f"""
            SELECT SUM(IF(DATE(embedded_at) = @d, input_tokens, 0)) AS d, SUM(input_tokens) AS m
            FROM `{self.project}.vectors.article_embeddings`
            WHERE DATE(embedded_at) BETWEEN @m0 AND @d""", self.dparams())[0]
        d, m = (r["d"] or 0) / 1e6, (r["m"] or 0) / 1e6
        self.add("Vertex AI", "gemini-embedding-2 (article vectors)", round(d, 3), "M tokens",
                 tiered("gemini_embedding_2_mtok", 0, d), tiered("gemini_embedding_2_mtok", 0, m))

    # ---------------------------------------------------------------- Cloud Run jobs
    def cloud_run(self):
        base = f"https://run.googleapis.com/v2/projects/{self.project}/locations/{REGION}/jobs"
        for job in JOBS:
            spec = self.get(f"{base}/{job}")["template"]["template"]["containers"][0]["resources"]["limits"]
            cpu = float(spec.get("cpu", "1"))
            mem = spec.get("memory", "512Mi")
            gib = float(mem[:-2]) / 1024 if mem.endswith("Mi") else float(mem[:-2])
            sec_d = sec_m = 0.0
            token, done = None, False
            while not done:
                page = self.get(f"{base}/{job}/executions", pageSize=100, **({"pageToken": token} if token else {}))
                for e in page.get("executions", []):
                    if not e.get("startTime"):
                        continue
                    start = datetime.fromisoformat(e["startTime"].replace("Z", "+00:00"))
                    if start.date() < self.month_start:
                        done = True
                        continue
                    if start.date() > self.day:
                        continue
                    end = datetime.fromisoformat((e.get("completionTime") or e["startTime"]).replace("Z", "+00:00"))
                    s = (end - start).total_seconds() * int(e.get("taskCount", 1))
                    sec_m += s
                    if start.date() == self.day:
                        sec_d += s
                token = page.get("nextPageToken")
                done = done or not token
            rate = cpu * PRICES["run_jobs_vcpu_s"][1] + gib * PRICES["run_jobs_gib_s"][1]
            self.add("Cloud Run", f"job {job} ({cpu:g} vCPU, {gib:g} GiB)", round(sec_d / 60, 1),
                     "minutes", sec_d * rate, sec_m * rate)
        self.notes.append("Cloud Run is priced at list rates with no free tier (conservative); "
                          "durations include container start-up.")

    # ---------------------------------------------------------------- storage: GCS + Artifact Registry
    def storage(self):
        days = calendar.monthrange(self.day.year, self.day.month)[1]
        end = datetime.combine(self.day + timedelta(days=1), datetime.min.time(), timezone.utc)
        ts = self.get(f"https://monitoring.googleapis.com/v3/projects/{self.project}/timeSeries",
                      filter='metric.type="storage.googleapis.com/storage/total_bytes"',
                      **{"interval.startTime": (end - timedelta(days=2)).isoformat().replace("+00:00", "Z"),
                         "interval.endTime": end.isoformat().replace("+00:00", "Z")})
        for s in ts.get("timeSeries", []):
            bucket = s["resource"]["labels"]["bucket_name"]
            loc = s["resource"]["labels"].get("location", "").upper()
            cls = s["metric"]["labels"].get("storage_class", "STANDARD").upper()
            gib = float(s["points"][0]["value"].get("doubleValue", 0)) / GIB
            mo = gib * GCS_GIB_MO.get((loc, cls), 0.026)
            self.add("Cloud Storage", f"{bucket} ({cls.lower()})", round(gib, 2), "GiB stored",
                     mo / days, mo / days * self.day.day)
        repos = self.get(f"https://artifactregistry.googleapis.com/v1/projects/{self.project}"
                         f"/locations/{REGION}/repositories").get("repositories", [])
        total = sum(int(r.get("sizeBytes", 0)) for r in repos) / GIB
        mo = tiered("ar_storage_gib_mo", 0, total)
        self.add("Artifact Registry", f"job images ({len(repos)} repos)", round(total, 2), "GiB stored",
                 mo / days, mo / days * self.day.day)

    # ---------------------------------------------------------------- Pinecone
    def pinecone(self):
        key = os.getenv("PINECONE_API_KEY")
        if not key:
            self.notes.append("Pinecone: PINECONE_API_KEY not set, skipped.")
            return
        from pinecone import Pinecone
        pc = Pinecone(api_key=key)
        # New vectors per day = articles embedded for the first time over the last 7 days (a
        # re-embed overwrites its vector). Not cost_daily's saved counts: backfilled days hold
        # the count of the day the backfill ran. ~35 MB scanned.
        new_per_day = self.q(f"""
            SELECT COUNTIF(DATE(first) BETWEEN DATE_SUB(@d, INTERVAL 6 DAY) AND @d) / 7 AS n
            FROM (SELECT MIN(embedded_at) AS first FROM `{self.project}.vectors.article_embeddings`
                  GROUP BY article_id)""", [bigquery.ScalarQueryParameter("d", "DATE", self.day)])[0]["n"] or 0
        used = growth = 0.0  # bytes, bytes/day
        for ix in pc.list_indexes():
            stats = pc.Index(ix.name).describe_index_stats()
            n, per = stats.total_vector_count, 4 * stats.dimension + PINECONE_META_BYTES
            self.add("Pinecone", f"index {ix.name} (Starter plan)", n, "vectors", 0.0, 0.0)
            used += n * per
            growth += new_per_day * per
        self.add("Pinecone", "storage (estimated, Starter limit 2 GB)", round(used / 1e9, 3), "GB", 0.0, 0.0)
        msg = f"Pinecone storage ≈ {used / 1e9:.2f} of 2 GB ({used / PINECONE_LIMIT_BYTES:.0%})"
        if growth > 0:
            months = (PINECONE_LIMIT_BYTES - used) / growth / 30.4
            msg = (("WARNING: " if months < 2 else "") + msg +
                   f", growing {growth / 1e6:.1f} MB/day ({new_per_day:,.0f} new articles/day over 7 days):"
                   f" full in ~{months:.1f} months"
                   f" (~{self.day + timedelta(days=round(months * 30.4))})")
        self.notes.append(msg + ". Estimate: 4 B x dims + 682 B metadata per vector (measured 10-24). "
                          "At the limit writes fail with 429 (embed-sync too); paid plan has a $50/month minimum.")

    # ---------------------------------------------------------------- OpenAI
    def openai(self):
        key = os.getenv("OPENAI_ADMIN_KEY") or self._secret("openai-admin-key")
        if not key:
            self.notes.append("OpenAI: no admin key, not tracked.")
            return
        import requests
        start = int(datetime.combine(self.month_start, datetime.min.time(), timezone.utc).timestamp())
        for attempt in range(4):  # the Costs API returns a transient 429 now and then (10-24 email)
            r = requests.get("https://api.openai.com/v1/organization/costs", timeout=30,
                             headers={"Authorization": f"Bearer {key}"},
                             params={"start_time": start, "bucket_width": "1d", "limit": 31,
                                     "group_by": "line_item"})
            if r.status_code not in (429, 500, 502, 503, 504) or attempt == 3:
                break
            time.sleep(min(float(r.headers.get("retry-after") or 0) or 5 * 2 ** attempt, 60))
        r.raise_for_status()
        by_model: dict[str, list[float]] = {}
        for b in r.json().get("data", []):
            bday = datetime.fromtimestamp(b["start_time"], timezone.utc).date()
            if bday > self.day:
                continue
            for res in b["results"]:
                model = (res.get("line_item") or "other").split(",")[0]
                v = float(res["amount"]["value"])
                dm = by_model.setdefault(model, [0.0, 0.0])
                dm[1] += v
                if bday == self.day:
                    dm[0] += v
        for model, (d, m) in sorted(by_model.items(), key=lambda kv: -kv[1][1]):
            self.add("OpenAI", model, "", "", d, m)
        if not by_model:
            self.add("OpenAI", "all models", "", "", 0.0, 0.0)
        self.notes.append("OpenAI is its billed cost (Costs API); the last day can still grow for a few hours.")

    # ---------------------------------------------------------------- official GCP bill
    def billing_export(self):
        """Google's own cost + credits from the BigQuery billing export (on since 2026-10-23),
        as a check on the estimates above; notes only, so nothing is counted twice. A day's
        rows keep arriving for ~1.5 days, so it reports the day before the report day."""
        tables = [t.table_id for t in self.bq.list_tables(f"{self.project}.billing_export")
                  if t.table_id.startswith("gcp_billing_export_v1_")]
        if not tables:
            self.notes.append("Billing export: no table yet.")
            return
        d1 = self.day - timedelta(days=1)
        m0 = d1.replace(day=1)
        params = [bigquery.ScalarQueryParameter("m0", "DATE", m0), bigquery.ScalarQueryParameter("d1", "DATE", d1)]
        bill = self.q(f"""
            SELECT DATE(usage_start_time) = @d1 AS is_d1, MIN(DATE(usage_start_time)) AS first_day,
                   SUM(cost / currency_conversion_rate) AS cost,
                   SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0) / currency_conversion_rate) AS credits
            FROM `{self.project}.billing_export.{tables[0]}`
            WHERE _PARTITIONTIME >= TIMESTAMP(@m0) AND DATE(usage_start_time) BETWEEN @m0 AND @d1
            GROUP BY 1""", params)
        if not bill:
            self.notes.append(f"Billing export: no rows for {m0}..{d1} yet.")
            return
        day = next((r for r in bill if r["is_d1"]), {"cost": 0.0, "credits": 0.0})
        cost, credits = sum(r["cost"] for r in bill), sum(r["credits"] for r in bill)
        first = min(r["first_day"] for r in bill)
        est = self.q(f"""
            SELECT SUM(day_usd) AS usd FROM `{self.project}.monitoring.cost_daily`
            WHERE day = @d1 AND service NOT IN ('OpenAI', 'Pinecone')""", params)[0]["usd"]
        usd = lambda x: f"${abs(x):.2f}" if round(x, 2) >= 0 else f"-${abs(x):.2f}"  # noqa: E731, no "$-0.00"
        self.notes.append(
            f"Official GCP bill (billing export) for {d1}: {usd(day['cost'])} cost, {usd(day['credits'])} credits, "
            f"{usd(day['cost'] + day['credits'])} to pay"
            + (f" (this report's GCP estimate for that day: {usd(est)})" if est is not None else "")
            + f". {first}..{d1}: {usd(cost)} cost, {usd(cost + credits)} to pay.")

    def _secret(self, name: str) -> str:
        try:
            d = self.get(f"https://secretmanager.googleapis.com/v1/projects/{self.project}"
                         f"/secrets/{name}/versions/latest:access")
            return base64.b64decode(d["payload"]["data"]).decode().strip()
        except Exception:  # noqa: BLE001
            return ""

    # ---------------------------------------------------------------- run + render
    def run(self):
        for part in (self.bigquery_queries, self.bigquery_storage, self.gemini, self.cloud_run,
                     self.storage, self.pinecone, self.openai, self.billing_export):
            try:
                part()
            except Exception as e:  # noqa: BLE001 - one broken source must not kill the report
                self.notes.append(f"{part.__name__} FAILED: {type(e).__name__}: {str(e)[:200]}")
        return self

    def totals(self):
        d = sum(r["day_usd"] for r in self.rows)
        m = sum(r["mtd_usd"] for r in self.rows)
        # Run-rate, not MTD extrapolated: one-off spends (a backfill) would inflate that.
        return d, m, d * calendar.monthrange(self.day.year, self.day.month)[1]

    def text(self) -> str:
        d, m, proj = self.totals()
        out = [f"Cost report for {self.day} (UTC) - project {self.project}",
               f"Day ${d:.2f} | month to date ${m:.2f} | month at this day's rate ${proj:.2f}", ""]
        out.append(f"{'Service':<18} {'Item':<44} {'Usage (day)':>20} {'Day $':>8} {'MTD $':>8}")
        for r in self.rows:
            out.append(f"{r['service']:<18} {r['item'][:44]:<44} {_usage(r):>20}{r['day_usd']:>8.3f} {r['mtd_usd']:>8.3f}")
        out += [""] + [f"* {n}" for n in self.notes]
        return "\n".join(out)

    def html(self) -> str:
        d, m, proj = self.totals()
        e = html.escape
        trs = "".join(
            f"<tr><td>{e(r['service'])}</td><td>{e(r['item'])}</td>"
            f"<td style='text-align:right'>{e(_usage(r))}</td>"
            f"<td style='text-align:right'>${r['day_usd']:.3f}</td><td style='text-align:right'>${r['mtd_usd']:.3f}</td></tr>"
            for r in self.rows)
        notes = "".join(f"<li>{e(n)}</li>" for n in self.notes)
        return f"""<div style="font-family:Arial,sans-serif;font-size:14px;color:#222">
<h2 style="margin:0 0 4px">Daily cost report: {self.day} (UTC)</h2>
<p style="margin:0 0 12px;color:#555">Project {e(self.project)}</p>
<table style="border-collapse:collapse;margin-bottom:12px"><tr>
<td style="padding:8px 16px;background:#eef3fb"><b>Day</b><br><span style="font-size:20px">${d:.2f}</span></td>
<td style="padding:8px 16px;background:#eef3fb"><b>Month to date</b><br><span style="font-size:20px">${m:.2f}</span></td>
<td style="padding:8px 16px;background:#eef3fb"><b>Month at this day's rate</b><br><span style="font-size:20px">${proj:.2f}</span></td>
</tr></table>
<table cellpadding="5" style="border-collapse:collapse;border:1px solid #ccc">
<tr style="background:#f3f3f3"><th align="left">Service</th><th align="left">Item</th><th align="right">Usage (day)</th><th align="right">Day</th><th align="right">MTD</th></tr>
{trs}</table>
<ul style="color:#555">{notes}</ul>
<p style="color:#888;font-size:12px">Measured usage x list prices (GCP trial credit not subtracted). monitoring/cost_report.py</p></div>"""

    def email(self):
        user, pw = os.environ["SMTP_USER"], os.environ["SMTP_PASSWORD"].replace(" ", "")
        to = os.getenv("REPORT_TO", user)
        d, m, proj = self.totals()
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[news-pipeline] Cost {self.day}: day ${d:.2f}, month ${m:.2f} (rate ${proj:.2f}/mo)"
        msg["From"], msg["To"] = user, to
        msg.attach(MIMEText(self.text(), "plain", "utf-8"))
        msg.attach(MIMEText(self.html(), "html", "utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as s:
            s.login(user, pw)
            s.sendmail(user, [to], msg.as_string())

    def save(self):
        table = f"{self.project}.monitoring.cost_daily"
        schema = [bigquery.SchemaField("day", "DATE"), bigquery.SchemaField("service", "STRING"),
                  bigquery.SchemaField("item", "STRING"), bigquery.SchemaField("usage", "FLOAT"),
                  bigquery.SchemaField("unit", "STRING"), bigquery.SchemaField("day_usd", "FLOAT"),
                  bigquery.SchemaField("mtd_usd", "FLOAT")]
        t = bigquery.Table(table, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(field="day")
        self.bq.create_table(t, exists_ok=True)
        rows = [{**r, "day": self.day.isoformat(), "usage": float(r["usage"]) if r["usage"] != "" else None}
                for r in self.rows]
        cfg = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE",
                                     time_partitioning=bigquery.TimePartitioning(field="day"))
        # Loading into the day's partition replaces it: re-running a day is idempotent. Load jobs are free.
        self.bq.load_table_from_json(rows, f"{table}${self.day:%Y%m%d}", job_config=cfg).result()


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", type=date.fromisoformat,
                    default=datetime.now(timezone.utc).date() - timedelta(days=1))
    ap.add_argument("--email", action="store_true")
    ap.add_argument("--save", action="store_true")
    args = ap.parse_args()
    rep = Report(args.day, os.environ["GCP_PROJECT"]).run()
    print(rep.text())
    if args.save:
        rep.save()
        print(f"saved {len(rep.rows)} rows to monitoring.cost_daily")
    if args.email:
        rep.email()
        print("email sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
