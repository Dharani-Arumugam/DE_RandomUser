#!/bin/bash
set -e

# Install Python dependencies if requirements.txt exists
if [ -f "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install -r /opt/airflow/requirements.txt
fi
 # Only run DB initialization if this container is marked as initializer
if [[ "${_AIRFLOW_DB_MIGRATE}" == "true" ]]; then
  echo "Running Airflow database migration..."
  airflow db migrate
fi

# For Airflow 3.1 and above: use `airflow auth add-user`
if [[ "${_AIRFLOW_WWW_USER_CREATE}" == "true" ]]; then
  echo "[Entrypoint] Creating Airflow admin user using airflow auth..."

  airflow users create \
   --username "${_AIRFLOW_WWW_USER_USERNAME:-airflow}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD:-airflow}" \
    --email "admin@example.com" \
    --firstname "Airflow" \
    --lastname "Admin" \
    --role Admin || true
fi


#echo "Starting Airflow service: $@"ß
# Run the command passed in Docker Compose
exec "$@"