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
  _ = sql("DROP DATABASE IF EXISTS {} CASCADE".format(database_name))
  dbutils.fs.rm(home_directory, True)

# COMMAND ----------

import re
from pathlib import Path

# このノートブックで作成されるすべてのオブジェクトは、ユーザー固有のデータベースに登録されます。
useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = useremail.split('@')[0]

# データベースと同様に、実際のコンテンツを指定のパスに保存します。
home_directory = '/home/{}/aml'.format(username)
dbutils.fs.mkdirs(home_directory)

# データを別の場所に保存したい場合は、このセルを書き換えてください。
database_name = '{}_aml'.format(re.sub('\W', '_', username))

# ローカルディスクに一時データを保存するパス
temp_directory = "/tmp/{}/aml".format(username)

# COMMAND ----------

tear_down()

# COMMAND ----------

_ = sql(f"CREATE DATABASE IF NOT EXISTS {database_name} LOCATION '{home_directory}'")
Path(temp_directory).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

import re

config = {
  'db_transactions': f"{database_name}.transactions",
  'db_entities': f"{database_name}.entities",
  'db_dedupe': f"{database_name}.dedupe",
  'db_synth_scores': f"{database_name}.synth_scores",
  'db_structuring': f"{database_name}.structuring",
  'db_structuring_levels': f"{database_name}.structuring_levels",
  'db_roundtrips': f"{database_name}.roundtrips",
  'db_risk_propagation': f"{database_name}.risk_propagation",
  'db_streetview': f"{database_name}.streetview",
  'db_dedupe_records': f"{database_name}.dedupe_splink",
}

# COMMAND ----------

tables = set(sql("SHOW TABLES IN {}".format(database_name)).toPandas().set_index("tableName").index)

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
experiment_name = f"/Users/{useremail}/aml_experiment"
mlflow.set_experiment(experiment_name) 
