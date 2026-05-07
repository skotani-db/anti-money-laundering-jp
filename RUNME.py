# Databricks notebook source
# MAGIC %md このノートブックは、ソリューションアクセラレーターを実行するためのコンパニオンクラスターをセットアップします。また、ワークフローDAGを作成し、実行順序を示すワークフローも作成します。クラスターを使用してノートブックを対話的に実行することも、ワークフローを実行してこのソリューションアクセラレーターの実行方法を確認することも可能です。
# MAGIC 
# MAGIC このスクリプトで作成されるパイプライン、ワークフロー、クラスターはユーザー固有ではないため、別のユーザーがUIからワークフローやクラスターを変更した場合、変更後にこのスクリプトを再実行するとリセットされます。
# MAGIC 
# MAGIC **注意**: ジョブの実行が失敗した場合は、アクセラレーターノートブックで指定されている他の環境依存関係が設定されていることを確認してください。アクセラレーターによっては、追加のクラウドインフラやデータアクセスのセットアップが必要な場合があります。

# COMMAND ----------

# DBTITLE 0,ユーティリティパッケージのインストール
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

job_json = {
        "timeout_seconds": 14400,
        "max_concurrent_runs": 1,
        "tags": {
                "usage": "solacc_automation",
                "group": "FSI"
            },
        "tasks": [
            {
                "job_cluster_key": "aml_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"00_aml_context" # 標準APIとは異なる
                },
                "task_key": "aml_00",
                "description": ""
            },
            {
                "job_cluster_key": "aml_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"01_aml_network_analysis" # 標準APIとは異なる
                },
                "task_key": "aml_01",
                "depends_on": [
                    {
                        "task_key": "aml_00"
                    }
                ],
                "description": ""
            },
            {
                "job_cluster_key": "aml_cluster",
                "notebook_task": {
                    "notebook_path": f"02_aml_address_verification" # 標準APIとは異なる
                },
                "task_key": "aml_02",
                "depends_on": [
                    {
                        "task_key": "aml_01"
                    }
                ]
            },
            {
                "job_cluster_key": "aml_cluster",
                "notebook_task": {
                    "notebook_path": f"03_aml_entity_resolution" # 標準APIとは異なる
                },
                "task_key": "aml_03",
                "depends_on": [
                    {
                        "task_key": "aml_02"
                    }
                ]
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "aml_cluster",
                "new_cluster": {
                    "spark_version": "11.3.x-cpu-ml-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.formatCheck.enabled": "false"
                        },
                    "num_workers": 2,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"}, # 標準APIとは異なる
                    "custom_tags": {
                        "usage": "solacc_automation",
                        "group": "FSI"
                    },
                }
            }
        ]
    }

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)

# COMMAND ----------


