#!/usr/bin/env bash

# run tests through docker-compose
if [ -z "$CI" ]; then
    docker-compose build || exit 1
fi

docker-compose down -v
docker-compose up --abort-on-container-exit

# check last docker-compose status
# rabbit stops with 143 exit code
CODE=`docker-compose ps -q | xargs docker inspect -f '{{ .State.ExitCode }}' | grep -vE '(0|143)' | wc -l | tr -d ' '`

echo "Tests completed with exit code $CODE"

# exit with last code
exit $CODE
