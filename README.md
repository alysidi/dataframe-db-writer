# Bulk Copy JSON 5 minute data to TimescaleDB

## How it Works

1. Create a beaconid folder under `json\beacon`
2. Download 5 min aggregate data into the `json\beacon\beaconid` folder from this S3 bucket
https://s3.console.aws.amazon.com/s3/buckets/neurio-prd-historical-data/?region=us-east-1

## How to Run

### Start TimescaleDB Docker

in root directory
```
docker-compose up
```

### Create Hypertables and Continuous Aggregates

```
see db/pwrview.sql 
```

### Load Test Data 

```
cd json/beacon
mkdir BEACON1
cd BEACON1
aws s3 sync s3://neurio-prd-historical-data/2893ddf1-2e04-45f2-bc71-44a1d70ec838/ba12dc75-e3d1-44df-87b8-6e1cecf55fc1/ba12dc75-e3d1-44df-87b8-6e1cecf55fc1/0001001204A5/5-Minute/ .
```

### Bulk Load Data into TimescaleDB

```
python3 dataframe-db-writer.py
```
will take each beacon in `/json/beacon` and bulk insert into the hyepertable

Alternatively, you can use this script, also in `pwrview.sql` to bulk load data for testing
```
-- insert load test data - change the interval for subsequent runs as there is a unique index on (device_id, time)
INSERT INTO pwrview(timestamp,device_id,SoC,solar_energy_exportedToBattery_kWh) (
   SELECT extract(epoch FROM time ), device_id, random()*100, random()*100
      FROM generate_series((NOW() + interval '1 day') - interval '6 hour',(NOW() + interval '1 day'), '1s') AS time
      CROSS JOIN LATERAL (
         SELECT 'inverter' || host_id::text AS device_id 
            FROM generate_series(0,9) AS host_id
      ) h
   );
  ```

### Continuous Aggregates

Create the Daily and Hourly Real Time Aggregates

The hourly will refresh 30 min and the daily will refresh daily as denoted by the `timescaledb.refresh_interval`

The `refresh_lag` is set to 2 x the time_bucket window so it automatically collates new data along with the materialized data.
Data older than this `refresh_lag` will have to wait until the next job run for the continuous aggregate ( the `imescaledb.refresh_interval` )

```
-- continuous aggregate materialized view - 1 hour time bucket
CREATE VIEW pwrview_1h
   WITH (timescaledb.continuous, 
         timescaledb.refresh_lag = '7200',
         timescaledb.refresh_interval = '3600')
   AS
      SELECT 
         time_bucket(BIGINT '3600', timestamp) AS hour,
         device_id, 
         sum(solar_energy_exportedToBattery_kWh),
         avg(SoC)
         FROM pwrview
      GROUP BY hour, device_id;


-- continuous aggregate materialized view - 1 day time bucket
CREATE VIEW pwrview_1day
   WITH (timescaledb.continuous, 
         timescaledb.refresh_lag = '172800',
         timescaledb.refresh_interval = '86400')
   AS
      SELECT 
         time_bucket(BIGINT '86400', timestamp) AS day,
         device_id, 
         sum(solar_energy_exportedToBattery_kWh),
         avg(SoC)
         FROM pwrview
      GROUP BY day, device_id;
```




