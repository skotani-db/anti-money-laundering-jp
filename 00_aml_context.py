# Databricks notebook source
# MAGIC %md 
# MAGIC このノートブックシリーズは https://github.com/databricks-industry-solutions/anti-money-laundering でご覧いただけます。このソリューションアクセラレーターの詳細については、https://www.databricks.com/blog/2021/07/16/aml-solutions-at-scale-using-databricks-lakehouse-platform.html をご参照ください。

# COMMAND ----------

# MAGIC %md
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fs-lakehouse-logo.png width="600px">
# MAGIC 
# MAGIC [![DBU](https://img.shields.io/badge/DBU-L-yellow)]()
# MAGIC [![COMPLEXITY](https://img.shields.io/badge/COMPLEXITY-201-yellow)]()
# MAGIC 
# MAGIC *マネーロンダリング対策（AML）コンプライアンスは、金融機関の監督を目的として、米国およびグローバル全体で最重要規制課題の一つとなっています。デジタルバンキングへの移行が進む中、金融機関は毎日数十億件のトランザクションを処理しており、より厳格な支払い監視や強固な顧客確認（KYC）ソリューションが導入されているにもかかわらず、マネーロンダリングのリスクは日々拡大しています。本ソリューションでは、FSI（金融サービス業界）がLakehouseプラットフォーム上でエンタープライズ規模のAMLソリューションを構築する方法について、顧客との協業経験を共有します。このソリューションは、強力な監視機能を提供するだけでなく、現代のオンラインマネーロンダリング脅威のリアルに対応するための革新的な拡張・適応機能も備えています。グラフ分析、自然言語処理（NLP）、コンピュータービジョンを通じて、データとAIの世界におけるAML防止の多様な側面を明らかにします。*
# MAGIC 
# MAGIC ---
# MAGIC <anindita.mahapatra@databricks.com>, <ricardo.portilla@databricks.com>, <sri.ghattamaneni@databricks.com>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://databricks.com/wp-content/uploads/2021/07/aml-blog-img-1-a.png' width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved. このノートブックのソースコードは、Databricksライセンス [https://databricks.com/db-license-source] に基づいて提供されています。含まれているまたは参照されているすべてのサードパーティライブラリは、以下に記載するライセンスに従います。
# MAGIC 
# MAGIC | ライブラリ                              | 説明                    | ライセンス  | ソース                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | graphframes:graphframes                | グラフライブラリ         | Apache2    | https://github.com/graphframes/graphframes          |
# MAGIC | torch                                  | Pytorchライブラリ        | BSD        | https://pytorch.org/                                |
# MAGIC | Pillow                                 | 画像処理                 | HPND       | https://python-pillow.org/                          |
# MAGIC | Splink                                 | エンティティ結合         | MIT        | https://github.com/moj-analytical-services/splink   |
