# Databricks notebook source
# MAGIC %pip install splink==2.1.14

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

def tear_down():
  import shutil
  try:
    shutil.rmtree(temp_directory)
  except:
    pass
  _ = sql("DROP SCHEMA IF EXISTS {}.{} CASCADE".format(catalog, schema))

# COMMAND ----------

import re
from pathlib import Path

# このノートブックで作成されるすべてのオブジェクトは、ユーザー固有のスキーマに登録されます。
useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = useremail.split('@')[0]

# データを別の場所に保存したい場合は、このセルを書き換えてください。
database_name = '{}_aml'.format(re.sub('\W', '_', username))

# Unity Catalog用のカタログとスキーマ
catalog = 'main'
schema = database_name

# ローカルディスクに一時データを保存するパス
temp_directory = "/tmp/{}/aml".format(username)

# COMMAND ----------

tear_down()

# COMMAND ----------

_ = sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
_ = sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
_ = sql(f"USE CATALOG {catalog}")
_ = sql(f"USE SCHEMA {schema}")
Path(temp_directory).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

import re

config = {
  'db_transactions': f"{catalog}.{schema}.transactions",
  'db_entities': f"{catalog}.{schema}.entities",
  'db_dedupe': f"{catalog}.{schema}.dedupe",
  'db_synth_scores': f"{catalog}.{schema}.synth_scores",
  'db_structuring': f"{catalog}.{schema}.structuring",
  'db_structuring_levels': f"{catalog}.{schema}.structuring_levels",
  'db_roundtrips': f"{catalog}.{schema}.roundtrips",
  'db_risk_propagation': f"{catalog}.{schema}.risk_propagation",
  'db_streetview': f"{catalog}.{schema}.streetview",
  'db_dedupe_records': f"{catalog}.{schema}.dedupe_splink",
  'catalog': catalog,
  'schema': schema,
}

# COMMAND ----------

tables = set(sql("SHOW TABLES IN {}.{}".format(catalog, schema)).toPandas().set_index("tableName").index)

if len(tables) == 0:
  
  print("入力テーブル {} および {} を作成しています".format(config['db_transactions'], config['db_entities']))
  # サンプルレコードでテーブルを作成する
  spark \
    .read \
    .format("parquet") \
    .load("s3://db-gtm-industry-solutions/data/fsi/aml_introduction/txns.parquet") \
    .write \
    .saveAsTable(config['db_transactions'])
  
  spark \
    .read \
    .format("parquet") \
    .load("s3://db-gtm-industry-solutions/data/fsi/aml_introduction/entities.parquet") \
    .write \
    .saveAsTable(config['db_entities'])

  spark \
    .read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("s3://db-gtm-industry-solutions/data/fsi/aml_introduction/dedupe.csv") \
    .write \
    .saveAsTable(config['db_dedupe'])

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{useremail}/aml_experiment"
mlflow.set_experiment(experiment_name) 
