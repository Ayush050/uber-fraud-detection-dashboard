Create or replace database uber_fraud_db;

create or replace schema raw;
create or replace schema analytics;
create or replace schema utility;

// creating tables 
CREATE OR REPLACE TABLE uber_fraud_db.raw.trip_request (
 request_id STRING PRIMARY KEY,
 user_id STRING,
 driver_id STRING,
 request_time TIMESTAMP,
 pickup_lat FLOAT,
 pickup_long FLOAT,
 drop_lat FLOAT,
 drop_long FLOAT
); 


CREATE OR REPLACE TABLE uber_fraud_db.raw.trip_status (
 request_id STRING,
 status STRING, -- e.g., 'cancelled', 'completed'
 cancel_time TIMESTAMP, -- applicable if cancelled
 who_cancelled STRING, -- 'Driver', 'User', or NULL
 cancel_reason STRING -- e.g., 'Customer not reachable'
);


CREATE OR REPLACE TABLE uber_fraud_db.raw.driver_location (
 driver_id STRING,
 time_stamp TIMESTAMP,
 latitude FLOAT,
 longitude FLOAT
);


CREATE OR REPLACE TABLE uber_fraud_db.analytics.fraud_alerts (
 request_id STRING,
 driver_id STRING,
 cancel_time TIMESTAMP,
 pickup_distance FLOAT,
 drop_distance FLOAT,
 new_ride_distance_from_drop FLOAT,
 fraud_risk STRING,  -- 'HIGH_RISK', 'MEDIUM_RISK', etc.
 fraud_score INT
);


-- How my files are saved In AWS S3 is like this -
-- uber-data/
-- |─ trip_requests/
--  └── trip_request_sample.csv
-- |─ trip_status/
--  └── trip_status_sample.csv
-- |─ driver_location/
--  └── driver_location_sample.csv
-- And this is my URL - URL='s3://my-bucket/uber-data/' 


-- Creating file format
create or replace FILE FORMAT uber_fraud_db.utility.uber_common_csv_ff
TYPE =csv
field_delimiter=','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER=1
NULL_IF = ('NULL' , 'null');

ALTER FILE FORMAT uber_fraud_db.utility.uber_common_csv_ff
SET TIMESTAMP_FORMAT = 'MM/DD/YYYY HH24:MI';



-- Creating storage Integration
CREATE OR REPLACE STORAGE INTEGRATION my_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::279191904489:role/uber_fraud_detection_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://uber-analysis-bucket/uber-data/');

DESC integration my_s3_int;




-- Creating stage to get files
Create or replace STAGE uber_fraud_db.utility.uber_raw_stage
url='s3://uber-analysis-bucket/uber-data/'
STORAGE_INTEGRATION=my_s3_int
FILE_FORMAT=(FORMAT_NAME='uber_common_csv_ff');


desc stage uber_fraud_db.utility.uber_raw_stage;
list @uber_fraud_db.utility.uber_raw_stage;




----------- Creating pipe
Create or replace PIPE uber_fraud_db.utility.uber_trip_request_pipe
AUTO_INGEST= True
AS
copy into uber_fraud_db.raw.trip_request
from @uber_fraud_db.utility.uber_raw_stage/trip_requests/
file_format=(format_name='uber_fraud_db.utility.uber_common_csv_ff')
on_error='continue';


Create or replace PIPE uber_fraud_db.utility.uber_trip_status_pipe
AUTO_INGEST= True
AS
COPY INTO uber_fraud_db.raw.trip_status
FROM @uber_fraud_db.utility.uber_raw_stage/trip_status/
FILE_FORMAT = (FORMAT_NAME='uber_fraud_db.utility.uber_common_csv_ff', ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR = 'CONTINUE';


Create or replace PIPE uber_fraud_db.utility.uber_driver_location_pipe
AUTO_INGEST= True
AS
COPY INTO uber_fraud_db.raw.driver_location
FROM @uber_fraud_db.utility.uber_raw_stage/driver_location/
FILE_FORMAT = (FORMAT_NAME='uber_fraud_db.utility.uber_common_csv_ff')
ON_ERROR = 'CONTINUE';



desc pipe uber_fraud_db.utility.uber_trip_request_pipe;
desc pipe uber_fraud_db.utility.uber_trip_status_pipe;
desc pipe uber_fraud_db.utility.uber_driver_location_pipe;







-- Creating streams on each table
Create or replace STREAM uber_fraud_db.utility.trip_request_stream
on TABLE uber_fraud_db.raw.trip_request;

Create or replace STREAM uber_fraud_db.utility.trip_status_stream
on TABLE uber_fraud_db.raw.trip_status;

Create or replace STREAM uber_fraud_db.utility.driver_location_stream
on TABLE uber_fraud_db.raw.driver_location;




select * from uber_fraud_db.utility.trip_request_stream;
select * from uber_fraud_db.utility.trip_status_stream;
select * from uber_fraud_db.utility.driver_location_stream;



select * from uber_fraud_db.raw.trip_request;
select * from uber_fraud_db.raw.trip_status;
select * from uber_fraud_db.raw.driver_location;


//----------------- logic of our problem ------------------------------------

Create or replace PROCEDURE uber_detecting_fraud_proc()
RETURNS STRING
LANGUAGE SQL
AS
$$
  BEGIN
 
    Merge INTO uber_fraud_db.analytics.fraud_alerts as tgt
    USING (
        WITH cancelled_rides AS (
            SELECT tr.*, ts.cancel_time, ts.cancel_reason, ts.who_cancelled
            FROM raw.trip_request AS tr
            JOIN raw.trip_status AS ts ON tr.request_id = ts.request_id
            WHERE ts.status = 'cancelled' AND ts.who_cancelled = 'Driver'
        ),
        pickup_check AS (
            SELECT cr.request_id, cr.driver_id,
                   MIN(ST_DISTANCE(
                        TO_GEOGRAPHY('POINT(' || cr.pickup_long || ' ' || cr.pickup_lat || ')'),
                        TO_GEOGRAPHY('POINT(' || d.longitude   || ' ' || d.latitude   || ')')
                   )) AS pickup_distance_m
            FROM cancelled_rides AS cr
            JOIN raw.driver_location AS d
              ON cr.driver_id = d.driver_id
             AND d.timestamp BETWEEN cr.cancel_time - INTERVAL '2 MINUTE'
                                   AND cr.cancel_time + INTERVAL '2 MINUTE'
            GROUP BY cr.request_id, cr.driver_id
        ),
        drop_check AS (
            SELECT cr.request_id, cr.driver_id,
                   MIN(ST_DISTANCE(
                        TO_GEOGRAPHY('POINT(' || cr.drop_long || ' ' || cr.drop_lat || ')'),
                        TO_GEOGRAPHY('POINT(' || d.longitude   || ' ' || d.latitude   || ')')
                   )) AS drop_distance_m
            FROM cancelled_rides AS cr
            JOIN raw.driver_location AS d
              ON cr.driver_id = d.driver_id
             AND d.timestamp BETWEEN cr.cancel_time - INTERVAL '2 MINUTE'
                                   AND cr.cancel_time + INTERVAL '2 MINUTE'
            GROUP BY cr.request_id, cr.driver_id
        ),
        next_ride_check AS (
            SELECT cr.request_id AS cancelled_request_id, cr.driver_id,
                   nr.request_id AS next_request_id,
                   ST_DISTANCE(
                        TO_GEOGRAPHY('POINT(' || cr.drop_long || ' ' || cr.drop_lat || ')'),
                        TO_GEOGRAPHY('POINT(' || nr.pickup_long || ' ' || nr.pickup_lat || ')')
                   ) AS next_ride_distance_from_drop_m
            FROM cancelled_rides AS cr
            JOIN raw.trip_request AS nr
              ON cr.driver_id = nr.driver_id
             AND nr.request_time > cr.cancel_time
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cr.request_id ORDER BY nr.request_time ASC) = 1
        ), 
        risk_calc AS (
            SELECT 
                c.request_id,
                c.driver_id,
                c.cancel_time,
                p.pickup_distance_m,
                d.drop_distance_m,
                n.next_ride_distance_from_drop_m,
                (
                    (CASE WHEN p.pickup_distance_m < 200 THEN 1 ELSE 0 END) +
                    (CASE WHEN d.drop_distance_m < 200 THEN 1 ELSE 0 END) +
                    (CASE WHEN n.next_ride_distance_from_drop_m < 200 THEN 1 ELSE 0 END)
                ) AS match_count
            FROM cancelled_rides c
            LEFT JOIN pickup_check p 
                ON c.request_id = p.request_id
            LEFT JOIN drop_check d 
                ON c.request_id = d.request_id
            LEFT JOIN next_ride_check n 
                ON c.request_id = n.cancelled_request_id
        )
        SELECT 
            request_id,
            driver_id,
            cancel_time,
            pickup_distance_m,
            drop_distance_m,
            next_ride_distance_from_drop_m,
            CASE 
                WHEN match_count = 3 THEN 'HIGH_RISK'
                WHEN match_count = 2 THEN 'MEDIUM_RISK'
                WHEN match_count = 1 THEN 'LOW_RISK'
                ELSE 'NO_RISK'
            END AS fraud_risk,
            match_count AS fraud_score
        FROM risk_calc 
        ) AS src 
           on tgt.request_id = src.request_id
           
            WHEN MATCHED THEN UPDATE SET
                tgt.driver_id = src.driver_id,
                tgt.cancel_time = src.cancel_time,
                tgt.pickup_distance = src.pickup_distance_m,
                tgt.drop_distance = src.drop_distance_m,
                tgt.new_ride_distance_from_drop = src.next_ride_distance_from_drop_m,
                tgt.fraud_risk = src.fraud_risk,
                tgt.fraud_score = src.fraud_score

            WHEN NOT MATCHED THEN INSERT (
                request_id, driver_id, cancel_time, pickup_distance, drop_distance,
                new_ride_distance_from_drop, fraud_risk, fraud_score
            )
            VALUES (
                src.request_id, src.driver_id, src.cancel_time, src.pickup_distance_m, src.drop_distance_m,
                src.next_ride_distance_from_drop_m, src.fraud_risk, src.fraud_score
            );

      RETURN 'Driver frauds detected successfully';
      
   END;
$$;

// --- Creating task to automate this inserting part 

Create or replace TASK driver_cancellation_analysis_task
warehouse = compute_wh
schedule = '1 minute'
when system$stream_has_data('uber_fraud_db.utility.trip_request_stream')
  or system$stream_has_data('uber_fraud_db.utility..trip_status_stream')
  or system$stream_has_data('uber_fraud_db.utility.driver_location_stream')
AS 
   call uber_detecting_fraud_proc();

Alter TASK driver_cancellation_analysis_task RESUME;

Alter TASK driver_cancellation_analysis_task SUSPEND;





Select * from uber_fraud_db.analytics.fraud_alerts;