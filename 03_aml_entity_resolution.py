# Databricks notebook source
# MAGIC %md 
# MAGIC このノートブックシリーズは https://github.com/databricks-industry-solutions/anti-money-laundering でご覧いただけます。このソリューションアクセラレーターの詳細については、https://www.databricks.com/blog/2021/07/16/aml-solutions-at-scale-using-databricks-lakehouse-platform.html をご参照ください。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # データ重複排除
# MAGIC 
# MAGIC AMLにおける最後の問題カテゴリとして、エンティティ解決に焦点を当てます。この問題に取り組むオープンソースライブラリは多数あります。基本的なエンティティのファジーマッチングには、スケールで結合を実現するライブラリを紹介します。

# COMMAND ----------

# MAGIC %md
# MAGIC 司法省によって開発された [Splink](https://github.com/moj-analytical-services/splink) は、エンタープライズ規模でマッチングカラムおよびブロッキングルールを設定できるエンティティ結合フレームワークです。フィールド属性（組織名+郵送先住所など）を組み合わせることで、類似性を検出して一致するレコードを重複排除できます。Splinkはマッチ確率を割り当てることで機能します。このマッチ確率は、報告された住所、エンティティ名、またはトランザクション金額において属性が非常に類似しているトランザクション（不審なアクティビティの可能性を示す）を特定するために使用できます。エンティティ解決はアカウント情報の照合において非常に手作業が多くなりがちですが、このタスクを自動化し、Delta Lakeに情報を保存するオープンソースライブラリを持つことで、調査担当者のケース解決の生産性を大幅に向上させることができます。このブログ[投稿](https://databricks.com/blog/2021/05/24/machine-learning-based-item-matching-for-retailers-and-brands.html)で言及されているLocality-Sensitive Hashing（LSH）を使用したものなど、エンティティマッチングにはいくつかのオプションがあります。読者の皆さんには、用途に適したアルゴリズムを選択することをお勧めします。

# COMMAND ----------

# MAGIC %run ./config/aml_config

# COMMAND ----------

raw_records = spark.read.table(config['db_dedupe'])
display(raw_records)

# COMMAND ----------

# MAGIC %md
# MAGIC 上記に示すように、「The Bank of New York Mellon Corp.」と「BNY Mellon」の間には目で見てわかる類似点があります。人間の目には明らかですが、プログラム的に大規模に実施する場合は真の課題となります。Splinkはマッチ確率を割り当てることで機能します。このマッチ確率は、エンティティの属性が非常に類似しているトランザクション（報告された住所、エンティティ名、またはトランザクション金額における潜在的なアラートを示す）を特定するために使用できます。エンティティ解決では、説明の間で必要なペアワイズ比較の数を減らすため、「ブロッキング」と呼ばれる前処理ステップを実行します。これは類似したエンティティの説明をブロックにまとめ、同じブロック内の説明間でのみ比較を実行するものです。

# COMMAND ----------

settings = {
    "link_type": "dedupe_only",
    "blocking_rules": [
        "l.amount = r.amount",
    ],
    "comparison_columns": [
        {
            "col_name": "org_name",
            "term_frequency_adjustments": True},
        {
            "col_name": "address",
            "term_frequency_adjustments": True
        },
        {
            "col_name": "country"
        },
              {
            "col_name": "amount"
        }
    ]
}

from splink import Splink
linker = Splink(settings, raw_records, spark)
raw_records_entities = linker.get_scored_comparisons()
display(raw_records_entities.take(1))

# COMMAND ----------

# MAGIC %md 
# MAGIC 上記に示すように、NY Mellonの住所に不整合が見つかりました。「Canada Square, Canary Wharf, London, United Kingdom」と「Canada Square, Canary Wharf, London, UK」が類似しています。重複排除されたレコードをDeltaテーブルに保存し、AML調査に活用できます。

# COMMAND ----------

raw_records_entities.write.mode("overwrite").format("delta").saveAsTable(config['db_dedupe_records'])

# COMMAND ----------

model = linker.model
model.probability_distribution_chart()
model.bayes_factor_chart()
model.all_charts_write_html_file(f"{temp_directory}/splink_charts.html", overwrite=True)

# COMMAND ----------

# MAGIC %md Splinkライブラリが提供する統計やグラフをさらに詳しく見ていきましょう。以下のコマンドは、マッチ確率の算出方法についての洞察を提供します。Splinkは主に期待値最大化（Expectation Maximization）フレームワークを使用して、下図に示すようにレコードペアのマッチ確率を生成するための尤度関数を最大化します。期待値最大化は反復アルゴリズムなので、異なる反復でのマッチと非マッチも確認できます。

# COMMAND ----------

html_file_content = open(f"{temp_directory}/splink_charts.html", 'r').read()
displayHTML(html_file_content)

# COMMAND ----------

# MAGIC %md
# MAGIC 探索的データ分析を続けると、個別のマッチとそれぞれの説明も確認できます。以下の例では、組織名と住所でマッチングを行い、これらのレベルに対するマッチ確率が示されています。人物名、住所、預入金額でマッチングして疑わしいトランザクションの大量発生を検出するのも良い例です。

# COMMAND ----------

from splink.intuition import intuition_report
row_dict = raw_records_entities.toPandas().sample(1).to_dict(orient="records")[0]
print(intuition_report(row_dict, model))

# COMMAND ----------

# MAGIC %md
# MAGIC 最終的に、一致するレコードの重複排除に使用できる一意の識別子で元のデータセットを拡充できます。ここでも、AIで基礎データを変更することは望まず、AML調査チームが必要な措置を講じるための必要なコンテキスト・ユーティリティをすべて提供することを目指します。

# COMMAND ----------

df2 = spark.table(config['db_transactions'])
df2 = df2.withColumnRenamed("txn_id", "unique_id")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC エンティティレベルでの重複排除戦略と同様に、送金元と受取人の郵送先住所など複数のフィールドを重複排除できます。

# COMMAND ----------

settings = {
    "link_type": "dedupe_only",
    "blocking_rules": [
        "l.txn_amount = r.txn_amount",
    ],
    "comparison_columns": [
        
        {
            "col_name": "rptd_originator_address",
        }, 
        { 
            "col_name": "rptd_originator_name",
        }
    ]
}

from splink import Splink
linker = Splink(settings, df2, spark)
df2_e = linker.get_scored_comparisons()

# COMMAND ----------

from pyspark.sql.functions import * 
display(df2_e.filter( (col("rptd_originator_address_l") != '')).filter((col("rptd_originator_address_r") != '')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC このノートブックシリーズでは、AML調査に関連する様々な技術的概念を簡単に紹介しました。ネットワーク分析、コンピュータービジョン、エンティティ解決を通じて、意思決定を補完し調査時間を短縮するためのAIの必要性を示しました。調査の側面は軽めに扱いましたが、LakehouseアーキテクチャがAML分析においてアナリストを支援するための最もスケーラブルで汎用的なプラットフォームである理由を説明しました。これらすべての機能により、組織は専有AMLソリューションと比較してTCOを削減できます。組織は、最も規制が厳しい活動と見なされることが多いものを自動化しようとするのではなく、調査チームに追加コンテキストを提供することで、高度な分析を日常的なプロセスに組み込み始めることができます。AIを**拡張知能**として活用することで、アナリストが活用できるシンプルなダッシュボード機能を通じて、AIが主導するすべての洞察を簡単にパッケージ化できます。
# MAGIC 
# MAGIC <img src=https://brysmiwasb.blob.core.windows.net/demos/aml/aml_dashboard.png width=800>
