#!/usr/bin/env bash
mkdir /tmp/spark-events
docker run -v /tmp/spark-events:/tmp/spark-events -p 18080:18080 sparkhistoryserver