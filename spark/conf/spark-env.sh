#!/bin/bash

export SPARK_DAEMON_MEMORY=2g
export SPARK_WORKER_MEMORY=1.5g
export SPARK_WORKER_CORES=2
export SPARK_DAEMON_JAVA_OPTS="-XX:MaxDirectMemorySize=2g"
# Either remove this line completely (use default 60s)
# export SPARK_WORKER_OPTS="-Dspark.worker.timeout=600s"

# If you really want 600s, use a plain number (no 's'):
export SPARK_WORKER_OPTS="-Dspark.worker.timeout=600"
