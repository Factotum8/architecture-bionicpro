export CH_HOST=localhost
export CH_PORT=8123
export CH_USER=default
export CH_PASSWORD=
export CH_DATABASE=default
export REPORTS_PIPELINE=reports_etl

uvicorn main:app --host 0.0.0.0 --port 8000