#!/usr/bin/env bash
# wait-for-it.sh

set -e

TIMEOUT=15
QUIET=0
HOST=
PORT=
CMD=

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -h|--host)
    HOST="$2"
    shift # past argument
    shift # past value
    ;;
    -p|--port)
    PORT="$2"
    shift # past argument
    shift # past value
    ;;
    -t|--timeout)
    TIMEOUT="$2"
    shift # past argument
    shift # past value
    ;;
    -q|--quiet)
    QUIET=1
    shift # past argument
    ;;
    --)
    shift
    CMD="$@"
    break
    ;;
    *)    # unknown option
    >&2 echo "Unknown option $key"
    exit 1
    ;;
esac
done

if [ -z "$HOST" ] || [ -z "$PORT" ]; then
    >&2 echo "Error: Host and Port are required."
    exit 1
fi

if [ "$QUIET" -eq 0 ]; then
    >&2 echo "Waiting for $HOST:$PORT..."
fi

start_ts=$(date +%s)
while :
do
    (echo > /dev/tcp/$HOST/$PORT) >/dev/null 2>&1
    result=$?
    if [ $result -eq 0 ]; then
        break
    fi
    sleep 1
    current_ts=$(date +%s)
    if [ $((current_ts - start_ts)) -gt "$TIMEOUT" ]; then
        >&2 echo "Timeout occurred after $TIMEOUT seconds waiting for $HOST:$PORT"
        exit 1
    fi
done

if [ "$QUIET" -eq 0 ]; then
    >&2 echo "$HOST:$PORT is up - executing command"
fi

exec $CMD