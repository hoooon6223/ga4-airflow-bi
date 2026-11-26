"""GA4 Merch Shop ops dashboard - one data day per run.

    resolve_date -> check_raw (sensor) -> dbt_run -> dbt_test (quality gate) -> render_ok ----\
                                     \_______ any of these fails ______ render_blocked -> publish -> gate_result

Date mapping (the data is frozen in 2020-21, the calendar is not):
    data_date = 2021-01-11 + (run day in KST - GA4_GO_LIVE_DATE)
With GA4_GO_LIVE_DATE set, the DAG starts 42 days before go-live, so turning it on catches up
11/30 ~ 1/10 (= backfill) and then runs daily at 07:00 KST for 1/11 ~ 1/24 (= live, 14 days).
Without it the DAG is manual-only: trigger with {"data_date": "2020-12-17"} (and "target": "test"
for incident replays such as 2021-01-26, which write to ga4_mart_test and never to the real site).

dbt runs as `dbt run` then `dbt test` (not `dbt build`): a failing gate must not stop kpi_daily from
being written - the day is recorded and flagged as blocked instead (docs/01_design.md section 8).
"""
import logging
import os
from datetime import date, timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, dag, task

KST = pendulum.timezone("Asia/Seoul")
OP_START, OP_END = date(2020, 11, 30), date(2021, 1, 24)   # operating period (docs/01_design.md 2.2)
LIVE_DATA_START = date(2021, 1, 11)                         # first data day of the live window
BACKFILL_DAYS, LIVE_DAYS = (LIVE_DATA_START - OP_START).days, (OP_END - LIVE_DATA_START).days + 1  # 42, 14

GO_LIVE = os.environ.get("GA4_GO_LIVE_DATE") or None
DBT = "cd /opt/project/dbt && /opt/dbt-venv/bin/dbt"
PY = "cd /opt/project && /opt/dbt-venv/bin/python"
X = "ti.xcom_pull(task_ids='resolve_date')"

log = logging.getLogger(__name__)


def alert(context):
    """Failure alert. Logged by default; extend here to send Slack/e-mail."""
    ti = context["ti"]
    log.error("ALERT ga4_ops_daily: task %s failed for run %s", ti.task_id, context["run_id"])


if GO_LIVE:
    go_live = pendulum.datetime(*map(int, GO_LIVE.split("-")), tz=KST)
    schedule, start, end = "0 7 * * *", go_live.subtract(days=BACKFILL_DAYS), go_live.add(days=LIVE_DAYS - 1, hours=12)
else:
    schedule, start, end = None, pendulum.datetime(2026, 1, 1, tz=KST), None


@dag(
    dag_id="ga4_ops_daily",
    schedule=schedule,
    start_date=start,
    end_date=end,
    catchup=True,                 # Airflow 3 defaults to False; catchup is how the backfill happens
    max_active_runs=1,            # days in order: f_orders dedup and the 7-day baselines read earlier days
    default_args={"depends_on_past": False, "on_failure_callback": alert,
                  "retries": 1, "retry_delay": timedelta(minutes=2)},
    params={
        "data_date": Param(None, type=["null", "string"], description="YYYY-MM-DD. Empty = derived from the run date."),
        "target": Param("prod", enum=["prod", "test"], description="test = incident replay into ga4_mart_test"),
    },
    tags=["ga4", "dbt", "dashboard"],
    doc_md=__doc__,
)
def ga4_ops_daily():

    @task(retries=0)
    def resolve_date(**context) -> dict:
        p = context["params"]
        if p.get("data_date"):
            d = date.fromisoformat(p["data_date"])
        elif GO_LIVE:
            run_day = context["logical_date"].in_timezone(KST).date()
            d = LIVE_DATA_START + (run_day - date.fromisoformat(GO_LIVE))
        else:
            raise AirflowFailException("No data_date given and GA4_GO_LIVE_DATE is not set.")
        target = p.get("target") or "prod"
        if target == "prod" and not OP_START <= d <= OP_END:
            raise AirflowFailException(f"{d} is outside the operating period {OP_START}~{OP_END}. Use target=test.")
        log.info("data_date=%s target=%s", d, target)
        return {"data_date": d.isoformat(), "suffix": d.strftime("%Y%m%d"), "target": target,
                "dataset": "ga4_mart" if target == "prod" else "ga4_mart_test",
                "site": "/opt/project/site" if target == "prod" else "/opt/airflow/logs/site_test"}

    resolved = resolve_date()

    # Wait for the day's GA4 export table. In production the export lands at a different time each day,
    # so the sensor re-checks every 10 minutes (reschedule mode frees the worker slot between checks)
    # and gives up after 6 hours -> render_blocked publishes "검수 실패 (원천 미도착)".
    # An empty table that did arrive is caught later by DQ1 in dq_daily.
    check_raw = BigQueryTableExistenceSensor(
        task_id="check_raw",
        project_id="bigquery-public-data",
        dataset_id="ga4_obfuscated_sample_ecommerce",
        table_id="events_{{ " + X + "['suffix'] }}",
        gcp_conn_id="google_cloud_default",
        mode="reschedule",
        poke_interval=int(os.environ.get("GA4_RAW_POKE_SECONDS", 600)),
        timeout=int(os.environ.get("GA4_RAW_TIMEOUT_SECONDS", 6 * 3600)),
        retries=0,
    )

    vars_ = "--vars '{run_date: \"{{ " + X + "['data_date'] }}\"}' --target {{ " + X + "['target'] }}"
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} seed --profiles-dir . {vars_} && {DBT} run --profiles-dir . {vars_}")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test --profiles-dir . {vars_}", retries=0)

    render_args = "--date {{ " + X + "['data_date'] }} --dataset {{ " + X + "['dataset'] }} --site {{ " + X + "['site'] }}"
    render_ok = BashOperator(task_id="render_ok", bash_command=f"{PY} -m dashboard.render {render_args}")
    render_blocked = BashOperator(
        task_id="render_blocked",
        trigger_rule="one_failed",
        bash_command=f"{PY} -m dashboard.render {render_args} --blocked-reason '파이프라인 또는 품질 게이트 실패 (Airflow 실행 로그 확인)'",
    )

    publish = EmptyOperator(task_id="publish", trigger_rule="one_success")   # step 7: push site/ to GitHub Pages

    @task(trigger_rule="all_done", retries=0)
    def gate_result(**context):
        """Keep the run red when the day was blocked, even though the blocked page was published."""
        if context["ti"].xcom_pull(task_ids="render_blocked"):
            raise AirflowFailException("Quality gate blocked this day - blocked page published instead of KPIs.")

    resolved >> check_raw >> dbt_run >> dbt_test >> render_ok
    [check_raw, dbt_run, dbt_test] >> render_blocked
    [render_ok, render_blocked] >> publish >> gate_result()


ga4_ops_daily()
