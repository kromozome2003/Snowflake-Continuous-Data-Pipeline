# Snowflake-Continuous-Data-Pipeline
The purpose of this project if to demo how easy it is to create a continuous data pipeline with Snowflake.
We are going to :
* Consume KAFKA messages on the fly bu using the Snowflake [Kafka Sink Connector](https://docs.snowflake.net/manuals/user-guide/kafka-connector.html)
* Put the AVRO content of those KAFKA messages into a source Snowflake TABLES
* Build a continuous data pipeline that EXTRACT incrementally the AVRO messages and put the EXTRACTED data to a destination TABLE

![](/images/sf-continuous-data-pipeline.png)

## Prerequisites
* Docker version 1.11 or later is installed and running.
* Docker Compose is installed. Docker Compose is installed by default with Docker for Mac.
* Docker memory is allocated minimally at 8 GB. When using Docker Desktop for Mac, the default Docker memory allocation is 2 GB. You can change the default allocation to 8 GB in Docker > Preferences > Advanced.
* Git.
* Internet connectivity.
* Ensure you are on an Operating System currently supported by Confluent Platform.
* Networking and Kafka on Docker: Configure your hosts and ports to allow both internal and external components to the Docker network to communicate. For more details, see this article.

## Introduction
Kafka Connect is a framework for connecting Kafka with external systems, including databases. A Kafka Connect cluster is a separate cluster from the Kafka cluster. The Kafka Connect cluster supports running and scaling out connectors (components that support reading and/or writing between external systems).

The Kafka connector is designed to run in a Kafka Connect cluster to read data from Kafka topics and write the data into Snowflake tables. Snowflake provides two versions of the connector:

* A version for the Confluent package version of Kafka.
For more information about Kafka Connect, see https://docs.confluent.io/3.0.0/connect/.

* A version for the open source software (OSS) Apache Kafka package.
For more information about Apache Kafka, see https://kafka.apache.org/.

From the perspective of Snowflake, a Kafka topic produces a stream of rows to be inserted into a Snowflake table. In general, each Kafka message contains one row.

Kafka, like many message publish/subscribe platforms, allows a many-to-many relationship between publishers and subscribers. A single application can publish to many topics, and a single application can subscribe to multiple topics. With Snowflake, the typical pattern is that one topic supplies messages (rows) for one Snowflake table.

## To begin
### Retrieve this github repo
```
git clone https://github.com/kromozome2003/Snowflake-KafkaConnect-Quickstart.git
cd Snowflake-KafkaConnect-Quickstart
```

## Prepare a key pair (required by KafkaConnect)
### A more detailed tuto is available [here](https://docs.snowflake.net/manuals/user-guide/kafka-connector-install.html#using-key-pair-authentication)
First, generate an encrypted private key with aes256 (with a passphrase)
```
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -inform PEM -out rsa_key.p8
```
below is the file where your private key has been stored (to use in Confluent & Snowsql)
```
cat rsa_key.p8 | awk ' !/-----/ {printf $1}'
```
Then, generate a public key based on the private one
```
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```
below is the file where your public key has been stored (to associate w/Snowflake user : RSA_PUBLIC_KEY)
```
cat rsa_key.pub | awk '!/-----/{printf $1}'
```

## Snowflake Initial Setup (1-setup.sql)
In order to be able to consume messages from our Kafka Connector you need to creates some objects.
A more detailed procedure is available [Here](https://docs.snowflake.net/manuals/user-guide/kafka-connector-install.html)
* Create a Snowflake DB
* Create a Snowflake ROLE
* Create Snowflake WAREHOUSES (for admin & tasks purpose)
* Create a Snowflake USER

## Test the setup with SnowSQL
If snowsql is not installed yet please follow this [LINK](https://docs.snowflake.net/manuals/user-guide/snowsql-install-config.html)
In my case my snowflake account is `eu_demo32.eu-central-1`
```
snowsql -a eu_demo32.eu-central-1 -u kafka_demo --private-key-path rsa_key.p8
```

## Kafka Setup
We are going now to deploy the distributed streaming platform.
There are 3 options today :
* The open source to deploy & maintain manually
* The Confluent distribution which enable production-grade kafka deployment & services
* The PaaS service you can find in your Cloud Provider Service catalog (ie. AWS Kinesis, AZURE Event Hub, GCP Cloud Pub/Sub)

We are going to use the Confluent option.
A mode detailed tutorial is available [here](https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html)

### In a terminal, deploy the Confluent Quickstart Containers
```
docker-compose up -d --build
docker-compose ps
```

### Confluent Kafka is now Deployed in Docker, Up & Running.
Open a web browser (Chrome) and go to [http://localhost:9021/](http://localhost:9021/)

### Let's create some 'dummy' topics (in the same terminal)
```
docker-compose exec connect bash -c 'kafka-topics --create --topic pageviews --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181'
docker-compose exec connect bash -c 'kafka-topics --create --topic stock_trades --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181'
docker-compose exec connect bash -c 'kafka-topics --create --topic clickstream --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181'
docker-compose exec connect bash -c 'kafka-topics --create --topic orders --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181'
docker-compose exec connect bash -c 'kafka-topics --create --topic ratings --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181'
docker-compose exec connect bash -c 'kafka-topics --create --topic users --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181'
```

### Let's deploy Snowflake Kafka Connector for Confluent in the `connect` container
```
docker-compose exec connect bash -c 'confluent-hub install --no-prompt --verbose snowflakeinc/snowflake-kafka-connector:1.2.3'
```

### Restart the Kafka-Connect container after the install
```
docker-compose restart connect
```
You can see the log with : `docker-compose logs connect | grep -i snowflake`

### Lets create 7 connectors :
* 6 connectors to generate random AVRO messages (DatagenConnector, 1 per Topic)
* 1 for Snowflake Sink connector to consume & load those messages into your Snowflake KAFKA_DB (SnowflakeSinkConnector)

In the browser [http://localhost:9021/](http://localhost:9021/)

#### Snowflake Sink Connector
* Cluster 1 -> Connect -> `connect-default` -> Add Connector -> Upload connector config file -> `connector_snowflake.json`
* Update Snowflake Login Info -> Continue -> Launch (Should see : Running)

#### Datagen Connectors (1 per topic)
* Cluster 1 -> Connect -> `connect-default` -> Upload connector config file -> `connector_datagen_xxx.json`
* Continue -> Launch (Should see : Running)
* Cluster 1 -> Topics -> `xxx` -> Messages (messages should appears)

### Check if messages arrives in Snowflake table (logged as KAFKA_DEMO user)
You should see new Snowflake table created according to topics.
They are automatically created by the connector if they don't already exists.

### Then let's create some SECURE VIEWS to demonstrate how to manipulate semi-structured AVRO on the fly (2-create-some-views.sql)
This creates 6 SECURE VIEWS that extract AVRO messages into a structured format.

### Now let's see how incrementally those Kafka messages are inserted

```
USE ROLE KAFKA_CONNECTOR_ROLE;
USE DATABASE KAFKA_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE KAFKA_ADMIN_WAREHOUSE;

SHOW TABLES;

-- Pageviews
--- # of messages
SELECT count(*) FROM KAFKA_DB.PUBLIC.PAGEVIEWS;
--- display what's in the last 10 kafka messages
SELECT * FROM KAFKA_DB.PUBLIC.PAGEVIEWS LIMIT 10;
--- inserts (messages) from the last hour
SELECT * FROM TABLE(information_schema.copy_history(table_name=>'KAFKA_DB.PUBLIC.PAGEVIEWS', start_time=> dateadd(hours, -1, current_timestamp())));

-- Orders
--- # of messages
SELECT count(*) FROM KAFKA_DB.PUBLIC.ORDERS;
--- display what's in the last 10 kafka messages
SELECT * FROM KAFKA_DB.PUBLIC.ORDERS LIMIT 10;
--- inserts (messages) from the last hour
SELECT * FROM TABLE(information_schema.copy_history(table_name=>'KAFKA_DB.PUBLIC.ORDERS', start_time=> dateadd(hours, -1, current_timestamp())));

-- Ratings
--- # of messages
SELECT count(*) FROM KAFKA_DB.PUBLIC.RATINGS;
--- display what's in the last 10 kafka messages
SELECT * FROM KAFKA_DB.PUBLIC.RATINGS LIMIT 10;
--- inserts (messages) from the last hour
SELECT * FROM TABLE(information_schema.copy_history(table_name=>'KAFKA_DB.PUBLIC.RATINGS', start_time=> dateadd(hours, -1, current_timestamp())));

-- Stock_trades
--- # of messages
SELECT count(*) FROM KAFKA_DB.PUBLIC.STOCK_TRADES;
--- display what's in the last 10 kafka messages
SELECT * FROM KAFKA_DB.PUBLIC.STOCK_TRADES LIMIT 10;
--- inserts (messages) from the last hour
SELECT * FROM TABLE(information_schema.copy_history(table_name=>'KAFKA_DB.PUBLIC.STOCK_TRADES', start_time=> dateadd(hours, -1, current_timestamp())));

-- Users
--- # of messages
SELECT count(*) FROM KAFKA_DB.PUBLIC.USERS;
--- display what's in the last 10 kafka messages
SELECT * FROM KAFKA_DB.PUBLIC.USERS LIMIT 10;
--- inserts (messages) from the last hour
SELECT * FROM TABLE(information_schema.copy_history(table_name=>'KAFKA_DB.PUBLIC.USERS', start_time=> dateadd(hours, -1, current_timestamp())));

-- Clickstream
--- # of messages
SELECT count(*) FROM KAFKA_DB.PUBLIC.CLICKSTREAM;
--- display what's in the last 10 kafka messages
SELECT * FROM KAFKA_DB.PUBLIC.CLICKSTREAM LIMIT 10;
--- inserts (messages) from the last hour
SELECT * FROM TABLE(information_schema.copy_history(table_name=>'KAFKA_DB.PUBLIC.CLICKSTREAM', start_time=> dateadd(hours, -1, current_timestamp())));
```

## Build the continuous data pipeline (3-streams-and-tasks-setup.sql)
This will create the :
* STREAM that capture changes on a TABLE (INSERT, UPDATE, DELETE...)
* destination TABLE that will receive extracted data (1 per TOPIC with the naming convention : <topic>\_EXTRACTED)
* STORED PROCEDURE that will be called to extract AVRO data from the source TABLE to the <topic>\_EXTRACTED TABLE
* TASK that will be triggered every minute (if the STREAM is not empty) and call the STORED PROCEDURE

## Monitoring the Pipeline (4-monitor-pipeline.sql)
This will show the SQL command to run in order to list all TASKS and next running time.
I've also included a query to see where are in transit data

## Pausing the DEMO

### First, stop generating Kafka messages (from Confluent GUI)
* Cluster 1 -> Connect -> `connect-default` -> Clic on 1 connector to PAUSE
* Clic on the PAUSE button on the upper right
* Repeat for every Datagen to PAUSE
```
docker-compose stop
docker-compose ps
```

## Clean up (after pausing)

### Delete the Confluent containers (assuming it's already stopped)
```
docker-compose ps
docker-compose rm
```

### Then Clean created Snowflake objects (5-cleanup.sql)


Mike Uzan - Senior SE (EMEA/France)
mike.uzan@snowflake.com
+33621728792
