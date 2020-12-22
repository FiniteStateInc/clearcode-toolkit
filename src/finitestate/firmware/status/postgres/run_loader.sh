#!/bin/sh
[ -z $1 ] && printf "Usage: run_loader.sh STAGE\n" && exit 0
export AWS_DEFAULT_REGION=us-east-1
export STAGE="${STAGE:-$1}"
if [ -z "${STAGE}" ]; then
  echo "Must specify STAGE"
  exit 1
fi

# Runs Alembic against database and runs migration scripts in alembic/versions
alembic upgrade head

