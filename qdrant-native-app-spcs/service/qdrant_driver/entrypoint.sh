#!/bin/bash
set -e  # Exit on command errors
set -x  # Print each command before execution, useful for debugging
mkdir -p /artifacts
jupyter lab --generate-config
nohup jupyter lab --ip='*' --port=8888 --no-browser --allow-root --NotebookApp.password='' --NotebookApp.token='' & 
gunicorn --bind 0.0.0.0:9000 --workers 1 --timeout 0 search-snowflakeembed:app -k uvicorn.workers.UvicornWorker &
tail -f /dev/null