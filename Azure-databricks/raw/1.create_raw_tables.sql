-- Databricks notebook source
CREATE DATABASE if not exists f1_raw

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
circuteId int,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt int,
url string
)
USING csv
OPTIONs (path "/mnt/dsformula1/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
raceId int,
year int,
round int,
circuitId int,
name string,
date date,
time string,
url string
)
using csv
options (path "/mnt/dsformula1/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Constructors table

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
   constructorId int,
   constructorRef string,
   name string,
   nationality string,
   url string)
 using json
 options (path "/mnt/dsformula1/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId int,
driverRef string,
number int,
code string,
name struct<forename: string, surname: string>,
dob date,
nationality string,
url string
)
using json
options (path "/mnt/dsformula1/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### create results table

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId int,
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points int,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId string
)
using json
options (path "/mnt/dsformula1/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
raceId int,
driverId int,
stop string,
lap int,
time string,
duration string,
milliseconds int
)
using json
options (path "/mnt/dsformula1/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create lap times table

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId int,
driverId int,
lap int,
position int,
time string,
milliseconds int
)
using csv
options (path "/mnt/dsformula1/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### create Qualifying table

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
qualifyId int,
raceId int,
driverId int,
constructorId int,
number int,
position int,
q1 string,
q2 string,
q3 string
)
using json
options (path "/mnt/dsformula1/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc table f1_raw.qualifying

-- COMMAND ----------


