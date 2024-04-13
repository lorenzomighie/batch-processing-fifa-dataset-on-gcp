SELECT * FROM `diesel-air-394514.fifa_schema.fifa_transformed_dataset` LIMIT 100;

-- Create a partitioned table
CREATE OR REPLACE TABLE diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned
PARTITION BY
  RANGE_BUCKET(version, GENERATE_ARRAY(17, 22, 1))
  AS
SELECT * FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned_clustered
PARTITION BY
  RANGE_BUCKET(version, GENERATE_ARRAY(17, 22, 1))
CLUSTER BY gender, club_name
  AS
SELECT * FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset;

CREATE OR REPLACE TABLE diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Impact of partition
SELECT *
FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned
WHERE version = 17;
-- 2.69 MB estimated 2.69 MB effective

SELECT *
FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset
WHERE version = 17;
-- 21.76 MB estimated 21.76 MB effective



--impact of clustering
SELECT *
FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned
WHERE version = 17 and gender = 'male' and club_name = 'Chelsea';
-- 2.69 MB estimated and effective

--impact of clustering (seems none)
SELECT *
FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned_clustered
WHERE version = 17 and gender = 'male' and club_name = 'Chelsea';
-- 2.69 MB estimated and effective

