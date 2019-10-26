import pyspark
import pyspark.sql.functions as f
import yaml
import os

spark = pyspark.sql.SparkSession.builder\
        .config('spark.jars.packages', 'io.delta:delta-core_2.11:0.4.0')\
        .getOrCreate()

sqlCtx = pyspark.sql.SQLContext(spark)

def extract(data_dir_path, no_partitions=10):
    gh_df = sqlCtx.read.json(data_dir_path).repartition(no_partitions)
    return gh_df.repartition(no_partitions, 'created_at')

def count_when(column, value):
    '''Count for given event type'''
    return f.count(f.when(f.col(column) == value, 1).otherwise(0)).alias(value)


def transfom_facts(df, events):
    events_agg = [count_when('type', e) for e in events]
    repo_fact = gh_df.repartition(10, 'repo').groupby('repo')\
            .agg(
                gh_df['repo.name'],
                *events_agg
            )
    return repo_fact

def load_to_delta_lakes(df, table_path):
    df.write.format('delta').save(table_path)
    return table_path


def load_config():
    base_path = os.getcwd()
    config_file = os.path.join(base_path, 'config.yml')
    with open(config_file, 'r') as f:
        return yaml.full_load(f)

if __name__ == '__main__':
    config = load_config()
    data_config = config.get('data')
    gh_df = extract(
            data_config.get('src_path'),
            data_config.get('num_partitions')
            )
    facts = transfom_facts(gh_df, config.get('events'))
    load_to_delta_lakes(facts, data_config.get('target_path'))
