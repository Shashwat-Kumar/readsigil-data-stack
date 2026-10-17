#!/usr/bin/env bash
# Install Apache Airflow with its official constraints file, then the Harbour kit.
set -euo pipefail
AIRFLOW_VERSION=3.2.2
PY=$(python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
pip install -e '.[test]'
