-- Set the context
USE ROLE KAFKA_CONNECTOR_ROLE;
USE DATABASE KAFKA_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE KAFKA_ADMIN_WAREHOUSE;

-- Create a STREAMS to monitor Data Changes (CDC) against RAW KAFKA tables
CREATE OR REPLACE STREAM STREAM_PAGEVIEWS ON TABLE PAGEVIEWS;
CREATE OR REPLACE STREAM STREAM_ORDERS ON TABLE ORDERS;
CREATE OR REPLACE STREAM STREAM_RATINGS ON TABLE RATINGS;
CREATE OR REPLACE STREAM STREAM_STOCK_TRADES ON TABLE STOCK_TRADES;
CREATE OR REPLACE STREAM STREAM_USERS ON TABLE USERS;
CREATE OR REPLACE STREAM STREAM_CLICKSTREAM ON TABLE CLICKSTREAM;

-- Display STREAMS
SHOW STREAMS;

-- Show STREAM records
SELECT * FROM STREAM_PAGEVIEWS LIMIT 5;
SELECT * FROM STREAM_ORDERS LIMIT 5;
SELECT * FROM STREAM_RATINGS LIMIT 5;
SELECT * FROM STREAM_STOCK_TRADES LIMIT 5;
SELECT * FROM STREAM_USERS LIMIT 5;
SELECT * FROM STREAM_CLICKSTREAM LIMIT 5;

-- Build a TABLE to host extracted avro data from STOCK_TRADES table
CREATE OR REPLACE TABLE PAGEVIEWS_EXTRACTED (
  CREATE_TIME timestamp,
  PAGEID string,
  USERID string,
  VIEWTIME string
);
CREATE OR REPLACE TABLE ORDERS_EXTRACTED (
  CREATE_TIME timestamp,
  CITY string,
  STATE string,
  ZIPCODE int,
  ITEMID string,
  ORDERID int,
  ORDERTIME int,
  ORDERUNITS float
);
CREATE OR REPLACE TABLE RATINGS_EXTRACTED (
  CREATE_TIME timestamp,
  CHANNEL string,
  MESSAGE string,
  RATINGID int,
  RATINGTIME int,
  ROUTEID int,
  STARS int,
  USERID int
);
CREATE OR REPLACE TABLE STOCK_TRADES_EXTRACTED (
  CREATE_TIME timestamp,
  ACCOUNT string,
  PRICE int,
  QUANTITY int,
  SIDE string,
  SYMBOL string,
  USERID string
);
CREATE OR REPLACE TABLE USERS_EXTRACTED (
  CREATE_TIME timestamp,
  GENDER string,
  REGIONID string,
  REGISTERTIME int,
  USERID string
);
CREATE OR REPLACE TABLE CLICKSTREAM_EXTRACTED (
  CREATE_TIME timestamp,
  _TIME string,
  AGENT string,
  BYTES string,
  IP string,
  REFERRER string,
  REMOTE_USER string,
  REQUEST string,
  STATUS string,
  TIME string,
  USERID string
);

-- Create a STORED PROCEDURE that will read the content of RAW table <TOPIC> and extract inserted AVRO data to table <TOPIC>_EXTRACTED
CREATE OR REPLACE PROCEDURE SP_EXTRACT_AVRO_CLICKSTREAM()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO CLICKSTREAM_EXTRACTED (CREATE_TIME,_TIME,AGENT,BYTES,IP,REFERRER,REMOTE_USER,REQUEST,STATUS,TIME,USERID)";
    sql_command += "    SELECT";
    sql_command += "        DATEADD(MS,RECORD_METADATA:CreateTime,'1970-01-01') CREATE_TIME,"
    sql_command += "        RECORD_CONTENT:_time::string _TIME,";
    sql_command += "        RECORD_CONTENT:agent::string AGENT,";
    sql_command += "        RECORD_CONTENT:bytes::string BYTES,";
    sql_command += "        RECORD_CONTENT:ip::string IP,";
    sql_command += "        RECORD_CONTENT:referrer::string REFERRER,";
    sql_command += "        RECORD_CONTENT:remote_user::string REMOTE_USER,";
    sql_command += "        RECORD_CONTENT:request::string REQUEST,";
    sql_command += "        RECORD_CONTENT:status::string STATUS,";
    sql_command += "        RECORD_CONTENT:time::string TIME,";
    sql_command += "        RECORD_CONTENT:userid::string USERID";
    sql_command += "    FROM STREAM_CLICKSTREAM";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "AVRO extracted.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_AVRO_PAGEVIEWS()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO PAGEVIEWS_EXTRACTED (CREATE_TIME,PAGEID,USERID,VIEWTIME)";
    sql_command += "    SELECT";
    sql_command += "        DATEADD(MS,RECORD_METADATA:CreateTime,'1970-01-01') CREATE_TIME,"
    sql_command += "        RECORD_CONTENT:pageid::string PAGEID,";
    sql_command += "        RECORD_CONTENT:userid::string USERID,";
    sql_command += "        RECORD_CONTENT:viewtime::string VIEWTIME";
    sql_command += "    FROM STREAM_PAGEVIEWS";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "AVRO extracted.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_AVRO_ORDERS()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO ORDERS_EXTRACTED (CREATE_TIME,CITY,STATE,ZIPCODE,ITEMID,ORDERID,ORDERTIME,ORDERUNITS)";
    sql_command += "    SELECT";
    sql_command += "        DATEADD(MS,RECORD_METADATA:CreateTime,'1970-01-01') CREATE_TIME,"
    sql_command += "        RECORD_CONTENT:address:city::string CITY,";
    sql_command += "        RECORD_CONTENT:address:state::string STATE,";
    sql_command += "        RECORD_CONTENT:address:zipcode::int ZIPCODE,";
    sql_command += "        RECORD_CONTENT:itemid::string ITEMID,";
    sql_command += "        RECORD_CONTENT:orderid::int ORDERID,";
    sql_command += "        RECORD_CONTENT:ordertime::int ORDERTIME,";
    sql_command += "        RECORD_CONTENT:orderunits::float ORDERUNITS";
    sql_command += "    FROM STREAM_ORDERS";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "AVRO extracted.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_AVRO_RATINGS()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO RATINGS_EXTRACTED (CREATE_TIME,CHANNEL,MESSAGE,RATINGID,RATINGTIME,ROUTEID,STARS,USERID)";
    sql_command += "    SELECT";
    sql_command += "        DATEADD(MS,RECORD_METADATA:CreateTime,'1970-01-01') CREATE_TIME,"
    sql_command += "        RECORD_CONTENT:channel::string CHANNEL,";
    sql_command += "        RECORD_CONTENT:message::string MESSAGE,";
    sql_command += "        RECORD_CONTENT:rating_id::int RATINGID,";
    sql_command += "        RECORD_CONTENT:rating_time::int RATINGTIME,";
    sql_command += "        RECORD_CONTENT:route_id::int ROUTEID,";
    sql_command += "        RECORD_CONTENT:stars::int STARS,";
    sql_command += "        RECORD_CONTENT:user_id::int USERID";
    sql_command += "    FROM STREAM_RATINGS";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "AVRO extracted.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_AVRO_STOCK_TRADES()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO STOCK_TRADES_EXTRACTED (CREATE_TIME,ACCOUNT,PRICE,QUANTITY,SIDE,SYMBOL,USERID)";
    sql_command += "    SELECT";
    sql_command += "        DATEADD(MS,RECORD_METADATA:CreateTime,'1970-01-01') CREATE_TIME,"
    sql_command += "        RECORD_CONTENT:account::string ACCOUNT,";
    sql_command += "        RECORD_CONTENT:price::int PRICE,";
    sql_command += "        RECORD_CONTENT:quantity::int QUANTITY,";
    sql_command += "        RECORD_CONTENT:side::string SIDE,";
    sql_command += "        RECORD_CONTENT:symbol::string SYMBOL,";
    sql_command += "        RECORD_CONTENT:userid::string USERID";
    sql_command += "    FROM STREAM_STOCK_TRADES";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "AVRO extracted.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_AVRO_USERS()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO USERS_EXTRACTED (CREATE_TIME,GENDER,REGIONID,REGISTERTIME,USERID)";
    sql_command += "    SELECT";
    sql_command += "        DATEADD(MS,RECORD_METADATA:CreateTime,'1970-01-01') CREATE_TIME,"
    sql_command += "        RECORD_CONTENT:gender::string GENDER,";
    sql_command += "        RECORD_CONTENT:regionid::string REGIONID,";
    sql_command += "        RECORD_CONTENT:registertime::int REGISTERTIME,";
    sql_command += "        RECORD_CONTENT:userid::string USERID";
    sql_command += "    FROM STREAM_USERS";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "AVRO extracted.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

-- Create a TASK to look for newly inserted AVRO messages (from <TOPIC> table) every 1 minute
CREATE OR REPLACE TASK TASK_EXTRACT_AVRO_FROM_CLICKSTRERAM warehouse = KAFKA_TASK_WH SCHEDULE = '1 minute' WHEN system$stream_has_data('STREAM_CLICKSTREAM') AS CALL SP_EXTRACT_AVRO_CLICKSTREAM();
CREATE OR REPLACE TASK TASK_EXTRACT_AVRO_FROM_PAGEVIEWS warehouse = KAFKA_TASK_WH SCHEDULE = '1 minute' WHEN system$stream_has_data('STREAM_PAGEVIEWS') AS CALL SP_EXTRACT_AVRO_PAGEVIEWS();
CREATE OR REPLACE TASK TASK_EXTRACT_AVRO_FROM_ORDERS warehouse = KAFKA_TASK_WH SCHEDULE = '1 minute' WHEN system$stream_has_data('STREAM_ORDERS') AS CALL SP_EXTRACT_AVRO_ORDERS();
CREATE OR REPLACE TASK TASK_EXTRACT_AVRO_FROM_RATINGS warehouse = KAFKA_TASK_WH SCHEDULE = '1 minute' WHEN system$stream_has_data('STREAM_RATINGS') AS CALL SP_EXTRACT_AVRO_RATINGS();
CREATE OR REPLACE TASK TASK_EXTRACT_AVRO_FROM_STOCK_TRADES warehouse = KAFKA_TASK_WH SCHEDULE = '1 minute' WHEN system$stream_has_data('STREAM_STOCK_TRADES') AS CALL SP_EXTRACT_AVRO_STOCK_TRADES();
CREATE OR REPLACE TASK TASK_EXTRACT_AVRO_FROM_USERS warehouse = KAFKA_TASK_WH SCHEDULE = '1 minute' WHEN system$stream_has_data('STREAM_USERS') AS CALL SP_EXTRACT_AVRO_USERS();
SHOW TASKS;

-- Resume TASK to make it run
ALTER TASK TASK_EXTRACT_AVRO_FROM_CLICKSTRERAM RESUME;
ALTER TASK TASK_EXTRACT_AVRO_FROM_PAGEVIEWS RESUME;
ALTER TASK TASK_EXTRACT_AVRO_FROM_ORDERS RESUME;
ALTER TASK TASK_EXTRACT_AVRO_FROM_RATINGS RESUME;
ALTER TASK TASK_EXTRACT_AVRO_FROM_STOCK_TRADES RESUME;
ALTER TASK TASK_EXTRACT_AVRO_FROM_USERS RESUME;
