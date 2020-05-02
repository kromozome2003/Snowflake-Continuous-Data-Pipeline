-- Set the context
USE ROLE KAFKA_CONNECTOR_ROLE;
USE DATABASE KAFKA_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE KAFKA_ADMIN_WAREHOUSE;

-- How long do we have to wait for next run ?
SELECT timestampdiff(second, CURRENT_TIMESTAMP, scheduled_time) AS next_run, scheduled_time, CURRENT_TIMESTAMP, name, state FROM TABLE(information_schema.task_history()) WHERE state = 'SCHEDULED' ORDER BY completed_time DESC;

-- Monitor the # of records in transit
SELECT
    'CLICKSTREAM' TOPIC,
    (SELECT COUNT(*) FROM CLICKSTREAM) SOURCE_RECORDS,
    (SELECT COUNT(*) FROM STREAM_CLICKSTREAM) STREAM_RECORDS,
    (SELECT COUNT(*) FROM CLICKSTREAM_EXTRACTED) TARGET_RECORDS;
