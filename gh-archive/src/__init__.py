import pyspark
import pyspark.sql.functions as f
import yaml
import os

spark = pyspark.sql.SparkSession.builder\
        .config('spark.jars.packages', 'io.delta:delta-core_2.11:0.4.0')\
        .getOrCreate()

sqlCtx = pyspark.sql.SQLContext(spark)

def extract(data_dir_path, no_partitions):
    gh_df = sqlCtx.read.json(data_dir_path).repartition(10)
    return gh_df.repartition(no_partitions, 'created_at')

def count_when(column, value):
    '''Count for given event type'''
    return f.count(f.when(f.col(column) == value, 1).otherwise(0)).alias(value)


def transfom_facts(df):
    repo_fact = gh_df.repartition(10, 'repo').groupby('repo')\
            .agg(
                gh_df['repo.name'],
                count_when('type','ReleaseEvent'),
                count_when('type', 'PushEvent'),
                count_when('type', 'WatchEvent')
            )

def load_to_delta_lakes(df, table_path):
    df.write.format('delta').save('/tmp/github-facts')


