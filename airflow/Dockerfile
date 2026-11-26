FROM apache/airflow:3.3.2-python3.12

# dbt and the dashboard renderer live in their own virtualenv so their dependencies never
# clash with Airflow's own packages. Airflow only calls them as commands (BashOperator).
USER root
RUN python -m venv /opt/dbt-venv && chown -R airflow: /opt/dbt-venv
USER airflow
RUN /opt/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.12.7" "dbt-bigquery==1.12.4" "jinja2>=3.1" "google-cloud-bigquery>=3"
