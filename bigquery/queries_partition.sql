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


-- best player by year
SELECT version, short_name, gender, overall
FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned_clustered as f1
WHERE overall = 
( SELECT MAX(overall)
  FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned_clustered as f2
  WHERE f1.version = f2.version
)
ORDER BY version;


-- Best team for a selected version, wrt to the best 11 players
SELECT club_name, SUM(overall) total
FROM (
    SELECT version, gender, club_name, overall, ROW_NUMBER() OVER(PARTITION BY club_name ORDER BY overall DESC) rn
    FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned
    WHERE version = 22 and gender = 'male' and club_name <> 'unknown'
) x 
WHERE rn <= 11
GROUP BY club_name
ORDER BY total DESC
LIMIT 5;

-- show club names for female gender
SELECT club_name
FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned_clustered
WHERE gender = 'female'
GROUP BY club_name;
-- result --> all have 'unknown' value


-- Sum of the wage of the 100 most payed players year by year
SELECT version, SUM(wage_eur) total_wage
FROM (
    SELECT version, gender, club_name, wage_eur, ROW_NUMBER() OVER(PARTITION BY version ORDER BY wage_eur DESC) rn
    FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned_clustered
) x 
WHERE rn <= 100
GROUP BY version
ORDER BY version ASC;


-- Best potential team for a certain version and gender wrt to all (30) players of a team
SELECT club_name, SUM(potential) total
FROM (
    SELECT version, gender, club_name, potential, ROW_NUMBER() OVER(PARTITION BY club_name ORDER BY potential DESC) rn
    FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned
    WHERE version = 22 and gender = 'male' and club_name <> 'unknown'
) x 
WHERE rn <= 30
GROUP BY club_name
ORDER BY total DESC
LIMIT 5;

-- Best nation team for selected year and gender
SELECT nationality_name, SUM(overall) total
FROM (
    SELECT version, gender, nationality_name, overall, ROW_NUMBER() OVER(PARTITION BY nationality_name ORDER BY overall DESC) rn
    FROM diesel-air-394514.fifa_schema.fifa_transformed_dataset_partitioned
    WHERE version = 22 and gender = 'female' and nationality_name <> 'unknown'
) x 
WHERE rn <= 11
GROUP BY nationality_name
ORDER BY total DESC
LIMIT 5;
