import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, monotonically_increasing_id
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--local_run', default=True, required=True)
parser.add_argument('--produce_parquet', default=False, required=True)
parser.add_argument('--bucket', default='mage-zoomcamp-bucket-lm', required=False)
parser.add_argument('--dataset_name', default='diesel-air-394514', required=False)
parser.add_argument('--table_name', default='fifa_transformed_dataset', required=False)


args = parser.parse_args()

local = args.local_run
parquet = args.produce_parquet
bucket = args.bucket
bq_dataset = args.dataset_name
bq_table = args.table_name

columns_to_keep = [
    'sofifa_id',
    'version',
    'short_name',
    'overall',
    'age',
    'dob',
    'height_cm',
    'weight_kg',
    'nationality_name',
    'club_name',
    'league_name',
    'league_level',
    'potential',
    'value_eur',
    'wage_eur',
    'player_positions'
]

def create_spark_session() -> SparkSession:

    if local:
        spark = SparkSession.builder.master("local[*]").appName('fifa-transformation').getOrCreate()
    else:
        spark = SparkSession.builder.master("yarn").appName('dataproc-fifa').getOrCreate()
    
    print('Spark Session Crated Successfully')
    return spark


def read_data(spark: SparkSession, fpath: str) -> (DataFrame, DataFrame):
    
    if local:
        path = ''
    else:
        path = "gs://{}/".format(bucket)

    df_male = spark.read.option("header", "true").option("inferSchema", "true").csv(path + 'fifa-dataset-male.csv')
    df_female = spark.read.option("header", "true").option("inferSchema", "true").csv(path + 'fifa-dataset-female.csv')
    
    

    print('Data Read Succesfully')

    return df_male, df_female


def transform_df(df_male: DataFrame, df_female: DataFrame) -> DataFrame:
    
    df_male_select = df_male.select(columns_to_keep)
    df_female_select = df_female.select(columns_to_keep)

    # Add gender column
    df_male_select = df_male_select.withColumn('gender', lit('male'))
    df_female_select = df_female_select.withColumn('gender', lit('female'))

    # Union the male and female dataframes
    df_complete = df_male_select.union(df_female_select)

    # fill club_name to 'uknown' when na or null
    df_complete = df_complete.na.fill({'club_name': 'unknown'})

    # order dataset and assign id

    # orderBy dataframe in asc order with regard to the query that will be applied
    df_ordered = df_complete.orderBy(['version', 'gender', 'club_name', col("overall").desc()])
    # add new unique id
    df_final = df_ordered.withColumn("unique_id", monotonically_increasing_id()).drop('sofifa_id')
    
    print("Data Transformed Successfully")
    
    return df_final


def save_data(df_final: DataFrame):
    # now that dataframe is ready export it to parquet by partitionin for version
    # even if dataset is not > 1 GB, if that were the case, this partitioning is the proper one as 
    # when querying the data they will always be grouped by da version of fifa

    if parquet:
        if local:
            path = 'dataset/'
        else:
            path = 'gs://{}/fifa_dataset/'.format(bucket)

        df_final.write.partitionBy('version').mode('overwrite').format('parquet').save('dataset/')

    if not local:
        table_path = '{}.{}'.format(bq_dataset, bq_table)
        df_final.write.format('bigquery').option('table', 'table_path').save()

    print('Data Saved Successfully')



def main():
    print("Spark transformation started")

    spark = create_spark_session()

    # read data from gcs
    df_male, df_female = read_data(spark, '')

    # compute df tansformations
    df_final = transform_df(df_male, df_female)

    # save data to bigQuery and optionally to gcs as .parquet
    save_data(df_final)

    spark.stop()

    print("Job Success")


if __name__=="__main__":
    main()