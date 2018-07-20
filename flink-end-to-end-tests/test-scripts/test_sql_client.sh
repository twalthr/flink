#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/kafka-common.sh

SQL_TOOLBOX_JAR=$END_TO_END_DIR/flink-sql-client-test/target/SqlToolbox.jar
SQL_JARS_DIR=$END_TO_END_DIR/flink-sql-client-test/target/sql-jars

################################################################################
# Verify existing SQL jars
################################################################################

EXTRACTED_JAR=$TEST_DATA_DIR/extracted

mkdir -p $EXTRACTED_JAR

for SQL_JAR in $SQL_JARS_DIR/*.jar; do
  echo "Checking SQL JAR: $SQL_JAR"
  unzip $SQL_JAR -d $EXTRACTED_JAR > /dev/null

  # check for proper shading
  for EXTRACTED_FILE in $(find $EXTRACTED_JAR -type f); do

    if ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/org/apache/flink"* ]] && \
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/META-INF"* ]] && \
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/LICENSE"* ]] && \
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/NOTICE"* ]] ; then
      echo "Bad file in JAR: $EXTRACTED_FILE"
      exit 1
    fi
  done

  # check for proper proper factory
  if [ ! -f $EXTRACTED_JAR/META-INF/services/org.apache.flink.table.factories.TableFactory ]; then
    echo "No table factory found in JAR: $SQL_JAR"
    exit 1
  fi

  # clean up
  rm -r $EXTRACTED_JAR/*
done

################################################################################
# Run a table program
################################################################################

echo "Testing table program..."

function sql_cleanup() {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  stop_kafka_cluster

  stop_cluster
  stop_all_taskmanagers
}
trap sql_cleanup INT
trap sql_cleanup EXIT

# prepare Kafka
echo "Preparing Kafka..."

setup_kafka_dist

start_kafka_cluster

create_kafka_topic 1 1 test-input
create_kafka_topic 1 1 test-output

# put JSON data into Kafka
echo "Sending messages to Kafka..."
send_messages_to_kafka '{"timestamp": "2018-03-12 08:00:00", "user": "Alice", "event": { "type": "WARNING", "message": "This is a warning."}}' test-input
send_messages_to_kafka '{"timestamp": "2018-03-12 09:00:00", "user": "Bob", "event": { "type": "WARNING", "message": "This is another warning."}}' test-input
send_messages_to_kafka '{"timestamp": "2018-03-12 09:10:00", "user": "Alice", "event": { "type": "INFO", "message": "This is a info."}}' test-input
send_messages_to_kafka '{"timestamp": "2018-03-12 09:20:00", "user": "Steve", "event": { "type": "INFO", "message": "This is another info."}}' test-input
send_messages_to_kafka '{"timestamp": "2018-03-12 09:30:00", "user": null, "event": { "type": "WARNING", "message": "This is a bad message because the user is missing."}}' test-input
send_messages_to_kafka '{"timestamp": "2018-03-12 09:40:00", "user": "Bob", "event": { "type": "ERROR", "message": "This is an error."}}' test-input

# prepare Flink
echo "Preparing Flink..."

start_cluster
start_taskmanagers 1

# create session environment file
RESULT=$TEST_DATA_DIR/result
SQL_CONF=$TEST_DATA_DIR/sql-client-session.conf

cat > $SQL_CONF << EOF
tables:
  - name: JsonSourceTable
    type: source
    update-mode: append
    schema:
      - name: rowtime
        type: TIMESTAMP
        rowtime:
          timestamps:
            type: from-field
            from: timestamp
          watermarks:
            type: periodic-bounded
            delay: "60000"
      - name: user
        type: VARCHAR
      - name: event
        type: ROW(type VARCHAR, message VARCHAR)
    connector:
      type: kafka
      version: "0.10"
      topic: test-input
      startup-mode: earliest-offset
      properties:
        - key: zookeeper.connect
          value: localhost:2181
        - key: bootstrap.servers
          value: localhost:9092
    format:
      type: json
      json-schema: >
        {
          "type": "object",
          "properties": {
            "timestamp": {
              "type": "string"
            },
            "user": {
              "type": ["string", "null"]
            },
            "event": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                },
                "message": {
                  "type": "string"
                }
              }
            }
          }
        }
  - name: AvroBothTable
    type: both
    update-mode: append
    schema:
      - name: event_timestamp
        type: VARCHAR
      - name: user
        type: VARCHAR
      - name: message
        type: VARCHAR
    connector:
      type: kafka
      version: "0.10"
      topic: test-output
      startup-mode: earliest-offset
      properties:
        - key: zookeeper.connect
          value: localhost:2181
        - key: bootstrap.servers
          value: localhost:9092
    format:
      type: avro
      avro-schema: >
        {
          "namespace": "org.apache.flink.table.tests",
          "type": "record",
          "name": "NormalizedEvent",
            "fields": [
              {"name": "event_timestamp", "type": "string"},
              {"name": "user", "type": ["string", "null"]},
              {"name": "message", "type": "string"}
            ]
        }
  - name: CsvSinkTable
    type: sink
    update-mode: append
    schema:
      - name: event_timestamp
        type: VARCHAR
      - name: user
        type: VARCHAR
      - name: message
        type: VARCHAR
    connector:
      type: filesystem
      path: $RESULT
    format:
      type: csv
      fields:
        - name: event_timestamp
          type: VARCHAR
        - name: user
          type: VARCHAR
        - name: message
          type: VARCHAR

functions:
  - name: RegReplace
    from: class
    class: org.apache.flink.table.toolbox.StringRegexReplaceFunction
EOF

# submit table programs

read -r -d '' SQL_STATEMENT_1 << EOF
INSERT INTO AvroBothTable
  SELECT
    CAST(rowtime AS VARCHAR) AS event_timestamp,
    user,
    RegReplace(event.message, ' is ', ' was ') AS message
  FROM JsonSourceTable
  WHERE user IS NOT NULL
EOF

echo "Executing SQL: Kafka JSON -> Kafka Avro"
echo "$SQL_STATEMENT_1"

$FLINK_DIR/bin/sql-client.sh embedded \
  --library $SQL_JARS_DIR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_1"

read -r -d '' SQL_STATEMENT_2 << EOF
INSERT INTO CsvSinkTable
  SELECT *
  FROM AvroBothTable
EOF

echo "Executing SQL: Kafka Avro -> Filesystem CSV"
echo "$SQL_STATEMENT_2"

$FLINK_DIR/bin/sql-client.sh embedded \
  --library $SQL_JARS_DIR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_2"

echo "Waiting for CSV results..."
for i in {1..10}; do
  if [ -e $RESULT ]; then
    CSV_LINE_COUNT=`cat $RESULT | wc -l`
    if [ $((CSV_LINE_COUNT)) -eq 5 ]; then
      break
    fi
  fi
  sleep 5
done

check_result_hash "SQLClient" $RESULT "fa321ad6030e2217ed4e01c0e7916697"
