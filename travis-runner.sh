#!/usr/bin/env bash

# run tests through docker-compose
docker-compose up --abort-on-container-exit

# check last docker-compose status
CODE=`docker-compose ps -q | xargs docker inspect -f '{{ .State.ExitCode }}' | grep -v 0 | wc -l | tr -d ' '`

echo "Tests completed with exit code $CODE"

# exit with last code
exit $CODE
