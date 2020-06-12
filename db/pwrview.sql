CREATE TABLE pwrview (
timestamp BIGINT  NOT NULL,
device_id TEXT NOT NULL,
SoC DOUBLE PRECISION  NULL,
averageEnergyCost DOUBLE PRECISION  NULL,
averageSoC DOUBLE PRECISION  NULL,
batteryCost DOUBLE PRECISION  NULL,
batteryCost_CRITICAL_PEAK DOUBLE PRECISION  NULL,
batteryCost_NOT_TOU DOUBLE PRECISION  NULL,
batteryCost_OFF_PEAK DOUBLE PRECISION  NULL,
batteryCost_ON_PEAK DOUBLE PRECISION  NULL,
batteryCost_PARTIAL_PEAK DOUBLE PRECISION  NULL,
batteryEnergy DOUBLE PRECISION  NULL,
batteryEnergy_CRITICAL_PEAK DOUBLE PRECISION  NULL,
batteryEnergy_NOT_TOU DOUBLE PRECISION  NULL,
batteryEnergy_OFF_PEAK DOUBLE PRECISION  NULL,
batteryEnergy_ON_PEAK DOUBLE PRECISION  NULL,
batteryEnergy_PARTIAL_PEAK DOUBLE PRECISION  NULL,
batteryPower DOUBLE PRECISION  NULL,
batteryPower_CRITICAL_PEAK DOUBLE PRECISION  NULL,
batteryPower_NOT_TOU DOUBLE PRECISION  NULL,
batteryPower_OFF_PEAK DOUBLE PRECISION  NULL,
batteryPower_ON_PEAK DOUBLE PRECISION  NULL,
batteryPower_PARTIAL_PEAK DOUBLE PRECISION  NULL,
batteryTouCumulativeCost DOUBLE PRECISION  NULL,
consumption_energy_fromBattery_kWh DOUBLE PRECISION  NULL,
consumption_energy_fromGrid_kWh DOUBLE PRECISION  NULL,
consumption_energy_fromSolar_kWh DOUBLE PRECISION  NULL,
consumption_power_fromBattery_kW DOUBLE PRECISION  NULL,
consumption_power_fromGrid_kW DOUBLE PRECISION  NULL,
consumption_power_fromSolar_kW DOUBLE PRECISION  NULL,
consumptionCost DOUBLE PRECISION  NULL,
consumptionCost_CRITICAL_PEAK DOUBLE PRECISION  NULL,
consumptionCost_NOT_TOU DOUBLE PRECISION  NULL,
consumptionCost_OFF_PEAK DOUBLE PRECISION  NULL,
consumptionCost_ON_PEAK DOUBLE PRECISION  NULL,
consumptionCost_PARTIAL_PEAK DOUBLE PRECISION  NULL,
consumptionEnergy DOUBLE PRECISION  NULL,
consumptionEnergy_CRITICAL_PEAK DOUBLE PRECISION  NULL,
consumptionEnergy_NOT_TOU DOUBLE PRECISION  NULL,
consumptionEnergy_OFF_PEAK DOUBLE PRECISION  NULL,
consumptionEnergy_ON_PEAK DOUBLE PRECISION  NULL,
consumptionEnergy_PARTIAL_PEAK DOUBLE PRECISION  NULL,
consumptionPower DOUBLE PRECISION  NULL,
consumptionPower_CRITICAL_PEAK DOUBLE PRECISION  NULL,
consumptionPower_NOT_TOU DOUBLE PRECISION  NULL,
consumptionPower_OFF_PEAK DOUBLE PRECISION  NULL,
consumptionPower_ON_PEAK DOUBLE PRECISION  NULL,
consumptionPower_PARTIAL_PEAK DOUBLE PRECISION  NULL,
consumptionTouCumulativeCost DOUBLE PRECISION  NULL,
cumulativeDailyCost DOUBLE PRECISION  NULL,
cumulativeDailySavings DOUBLE PRECISION  NULL,
currentBatteryEnergy DOUBLE PRECISION  NULL,
currentBatteryPower DOUBLE PRECISION  NULL,
currentConsumptionEnergy DOUBLE PRECISION  NULL,
currentConsumptionPower DOUBLE PRECISION  NULL,
currentGenerationEnergy DOUBLE PRECISION  NULL,
currentGenerationPower DOUBLE PRECISION  NULL,
currentNetEnergy DOUBLE PRECISION  NULL,
currentNetPower DOUBLE PRECISION  NULL,
efficiencyInOut DOUBLE PRECISION  NULL,
generationCost DOUBLE PRECISION  NULL,
generationCost_CRITICAL_PEAK DOUBLE PRECISION  NULL,
generationCost_NOT_TOU DOUBLE PRECISION  NULL,
generationCost_OFF_PEAK DOUBLE PRECISION  NULL,
generationCost_ON_PEAK DOUBLE PRECISION  NULL,
generationCost_PARTIAL_PEAK DOUBLE PRECISION  NULL,
generationEnergy DOUBLE PRECISION  NULL,
generationEnergy_CRITICAL_PEAK DOUBLE PRECISION  NULL,
generationEnergy_NOT_TOU DOUBLE PRECISION  NULL,
generationEnergy_OFF_PEAK DOUBLE PRECISION  NULL,
generationEnergy_ON_PEAK DOUBLE PRECISION  NULL,
generationEnergy_PARTIAL_PEAK DOUBLE PRECISION  NULL,
generationPower DOUBLE PRECISION  NULL,
generationPower_CRITICAL_PEAK DOUBLE PRECISION  NULL,
generationPower_NOT_TOU DOUBLE PRECISION  NULL,
generationPower_OFF_PEAK DOUBLE PRECISION  NULL,
generationPower_ON_PEAK DOUBLE PRECISION  NULL,
generationPower_PARTIAL_PEAK DOUBLE PRECISION  NULL,
generationTouCumulativeCost DOUBLE PRECISION  NULL,
kpi_averageSolarPerformance_kWh_Per_kWp DOUBLE PRECISION  NULL,
kpi_performanceRatio DOUBLE PRECISION  NULL,
kpi_solarPerformanceSoFar_kWh_Per_kWp DOUBLE PRECISION  NULL,
kpi_solarPerformance_kWh_Per_kWp DOUBLE PRECISION  NULL,
netCost DOUBLE PRECISION  NULL,
netCost_CRITICAL_PEAK DOUBLE PRECISION  NULL,
netCost_NOT_TOU DOUBLE PRECISION  NULL,
netCost_OFF_PEAK DOUBLE PRECISION  NULL,
netCost_ON_PEAK DOUBLE PRECISION  NULL,
netCost_PARTIAL_PEAK DOUBLE PRECISION  NULL,
netEnergy DOUBLE PRECISION  NULL,
netEnergy_CRITICAL_PEAK DOUBLE PRECISION  NULL,
netEnergy_NOT_TOU DOUBLE PRECISION  NULL,
netEnergy_OFF_PEAK DOUBLE PRECISION  NULL,
netEnergy_ON_PEAK DOUBLE PRECISION  NULL,
netEnergy_PARTIAL_PEAK DOUBLE PRECISION  NULL,
netPower DOUBLE PRECISION  NULL,
netPower_CRITICAL_PEAK DOUBLE PRECISION  NULL,
netPower_NOT_TOU DOUBLE PRECISION  NULL,
netPower_OFF_PEAK DOUBLE PRECISION  NULL,
netPower_ON_PEAK DOUBLE PRECISION  NULL,
netPower_PARTIAL_PEAK DOUBLE PRECISION  NULL,
numDataPoints DOUBLE PRECISION  NULL,
savings DOUBLE PRECISION  NULL,
savings_CRITICAL_PEAK DOUBLE PRECISION  NULL,
savings_NOT_TOU DOUBLE PRECISION  NULL,
savings_OFF_PEAK DOUBLE PRECISION  NULL,
savings_ON_PEAK DOUBLE PRECISION  NULL,
savings_PARTIAL_PEAK DOUBLE PRECISION  NULL,
solar_energy_exportedToBattery_kWh DOUBLE PRECISION  NULL,
solar_energy_exportedToGrid_kWh DOUBLE PRECISION  NULL,
solar_energy_exportedToHome_kWh DOUBLE PRECISION  NULL,
solar_power_exportedToBattery_kW DOUBLE PRECISION  NULL,
solar_power_exportedToGrid_kW DOUBLE PRECISION  NULL,
solar_power_exportedToHome_kW DOUBLE PRECISION  NULL,
totalSavings DOUBLE PRECISION  NULL,
touPeriod TEXT NULL,
start BIGINT NULL
);


-- create hypertable with 1 day partitions ( default is 7 days )
SELECT create_hypertable('pwrview', 'timestamp', chunk_time_interval => 86400);

-- add time function for intervals in epochs. default is to use timestampz
CREATE OR REPLACE FUNCTION unix_now() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT extract(epoch from now())::BIGINT $$;

-- set timestamp to epoch
SELECT set_integer_now_func('pwrview', 'unix_now');

-- to avoid duplicates and fast lookups by device
CREATE UNIQUE INDEX on pwrview (device_id, timestamp desc);



-- insert load test data
INSERT INTO pwrview(timestamp,device_id,SoC,solar_energy_exportedToBattery_kWh) (
   SELECT extract(epoch FROM time ), device_id, random()*100, random()*100
      FROM generate_series((NOW() - interval '5 day') - interval '6 hour',(NOW() - interval '5 day'), '1s') AS time
      CROSS JOIN LATERAL (
         SELECT 'beacon' || host_id::text AS device_id 
            FROM generate_series(0,9) AS host_id
      ) h
   );


-- continuous aggregate materialized view - 1 hour time bucket
CREATE VIEW pwrview_1h
   WITH (timescaledb.continuous, 
         timescaledb.refresh_lag = '3600',
         timescaledb.refresh_interval = '1800')
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
         timescaledb.refresh_lag = '86400',
         timescaledb.refresh_interval = '86400')
   AS
      SELECT 
         time_bucket(BIGINT '86400', timestamp) AS day,
         device_id, 
         sum(solar_energy_exportedToBattery_kWh),
         avg(SoC)
         FROM pwrview
      GROUP BY day, device_id;

-- base queries
 
 insert into pwrview(device_id, timestamp, SoC, solar_energy_exportedToBattery_kWh) values('ZZZZZZ',1591939294, 100, 100)

 select count(*) from pwrview;

 select to_timestamp(timestamp), * from pwrview order by timestamp asc limit 1000;

 select to_timestamp(hour), * from pwrview_1h order by hour asc;

 select to_timestamp(day), * from pwrview_1day order by day asc;

 select * FROM hypertable_approximate_row_count('pwrview');

-- analyze queries

 explain analyze select to_timestamp(timestamp),* from pwrview where device_id='beacon0' and timestamp<1591318800 and timestamp>1591314800

 explain analyze select * from pwrview_1day where device_id='AXE33333' and day < 1691920000 and day > 1490883200 order by day asc;

 explain analyze select to_timestamp(hour),* from pwrview_1h where device_id='beacon0' and hour < 1691920000 and hour > 1390883200 order by hour asc;

 explain analyze select to_timestamp(day),* from pwrview_1day where device_id='beacon0' order by day asc; 



select to_timestamp(min(timestamp)), to_timestamp(max(timestamp)) from pwrview where device_id='beacon0';

REFRESH MATERIALIZED VIEW pwrview_1h;
REFRESH MATERIALIZED VIEW pwrview_1day;


-- Refresh Interval for Rollups
ALTER VIEW pwrview_1day SET (
  timescaledb.refresh_interval = '60'
   )

-- Change Materialization
ALTER VIEW pwrview_1day SET (
  timescaledb.materialized_only = true
   )

-- Data Rention 
ALTER VIEW pwrview_1h SET (
  timescaledb.materialized_only = false,
     timescaledb.ignore_invalidation_older_than = 86400 )

ALTER VIEW pwrview_1day SET (
  timescaledb.materialized_only = false,
  timescaledb.ignore_invalidation_older_than = 86400)

SELECT drop_chunks(older_than => 1591660800, table_name => 'pwrview', cascade_to_materializations => FALSE);

-- Compression

ALTER TABLE pwrview SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device_id'
);

SELECT add_compress_chunks_policy('pwrview', 86400);

SELECT compress_chunk( '_timescaledb_internal._hyper_11_848_chunk');


