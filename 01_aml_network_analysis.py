# Databricks notebook source
# MAGIC %md 
# MAGIC このノートブックシリーズは https://github.com/databricks-industry-solutions/anti-money-laundering でご覧いただけます。このソリューションアクセラレーターの詳細については、https://www.databricks.com/blog/2021/07/16/aml-solutions-at-scale-using-databricks-lakehouse-platform.html をご参照ください。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # グラフパターン
# MAGIC 
# MAGIC AMLアナリストがケース調査に使用する主要なデータソースの一つとして、トランザクションデータがあります。このデータは表形式でSQLから容易にアクセスできますが、3層以上の深さのトランザクション連鎖をSQLクエリで追跡することは困難です。そのため、不正に取引する疑いのある人物のネットワークのような単純な概念を表現するために、柔軟な言語とAPIのスイートを持つことが重要です。幸いなことに、Databricks Runtime for Machine Learningにプレインストールされているグラフ API である [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html) を使用することで、これを簡単に実現できます。
# MAGIC 
# MAGIC トランザクションおよびトランザクションから導出されたエンティティで構成されるデータセットを活用して、Spark、GraphFrames、Delta Lakeでこれらのパターンの存在を検出します。検出されたパターンはDelta Lakeに保存されるため、Databricks SQLをゴールドレベルの集計結果に適用できます。パターンの核心的な価値は、アナリストが関連する個人の調査を統合できることです。各シナリオにおいて、グラフ分析を使用して個人の接続性を活用します。このような方法でアイデンティティを接続することで、ケースを統合し、作業の重複を減らし、ケース解決までの時間を短縮できます。

# COMMAND ----------

# MAGIC %run ./config/aml_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## エンティティ解決 / 合成ID
# MAGIC 
# MAGIC 合成IDの存在は警戒すべき要因となります。グラフ分析を使用することで、トランザクション内のすべてのエンティティを一括分析してリスクレベルを検出できます。エンティティ間に存在するコネクション（共通属性）の数に基づいてスコアを低くまたは高く設定し、高スコアのグループに基づいてアラートを生成できます。下図はこのアイデアの基本的な表現です。
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/AML-on-Lakehouse-Platform-blog-img-2.jpg" width=550/>
# MAGIC 
# MAGIC 本分析は3つのフェーズで実施します：
# MAGIC 
# MAGIC a) トランザクションデータからエンティティを抽出する
# MAGIC <br>
# MAGIC b) 住所、電話番号、メールアドレスに基づいてエンティティ間のリンクを作成する
# MAGIC <br>
# MAGIC c) GraphFramesの連結コンポーネントを使用して、複数のエンティティ（IDとその他の属性で識別）が1つ以上のリンクで接続されているかどうかを判定する

# COMMAND ----------

# MAGIC %md
# MAGIC **SQLを使用した場合**
# MAGIC 
# MAGIC グラフ理論に直接進む前に、まず標準SQLを使用して合成データセットを確認します。データセットを自己結合することで、メールアドレスなど同一の属性を共有するエンティティを簡単に抽出できます。1次または2次のつながりには対応できますが、より深い洞察を得るには、後述するより高度なネットワーク技法が必要です。

# COMMAND ----------

# DBTITLE 1,データベーストランザクション（後で使用）
display(spark.read.table(config['db_transactions']))

# COMMAND ----------

# DBTITLE 1,同一メールアドレスを持つエンティティ（表示のみ）
display(
  sql("""
  select * 
  from {0}
  where email_addr in 
  (
    select A.email_addr 
    from 
      (
        select email_addr, count(*) as cnt 
        from {0}
        group by email_addr
      ) A
    where A.cnt > 1
  )
  order by email_addr
  """.format(config['db_entities']))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### GraphFramesの使用
# MAGIC より深い関係性を探索しようとすると、SQLステートメントのサイズと複雑さが指数関数的に増大し、Graphframesのようなグラフライブラリが必要になります。[GraphFrames](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#basic-graph-and-dataframe-queries) は、Apache Spark向けのDataFrameベースのグラフパッケージです。Scala、Java、Pythonの高レベルAPIを提供し、GraphXの機能とSpark DataFramesの拡張機能（モチーフ検索、DataFrameベースのシリアライゼーション、高い表現力を持つグラフクエリなど）を組み合わせています。

# COMMAND ----------

from graphframes import *

# COMMAND ----------

# DBTITLE 1,グラフの構築
# MAGIC %md
# MAGIC ノードがトランザクションの各属性（メール/電話/住所）を表し、エッジがそれらの属性間の関係を表すグラフ構造を構築します。顧客AとメールアドレスEを含むトランザクションは、「ノード」Aと「ノード」Eを接続します。このグラフは無向・無重み付きのネットワークになります。
# MAGIC 
# MAGIC <img src="https://github.com/stephanieamrivera/upgraded-octo-parakeet/blob/main/slides/AML%20Example%20Graph.png?raw=true" width=850>

# COMMAND ----------

# DBTITLE 1,ノードとエッジのコーディング
identity_edges = sql('''
select entity_id as src, address as dst from {0} where address is not null
union
select entity_id as src, email_addr as dst from {0} where email_addr is not null
union
select entity_id as src, phone_number as dst from {0} where phone_number is not null
'''.format(config['db_entities']))

identity_nodes = sql('''
select distinct(entity_id) as id, 'Person' as type from {0}
union 
select distinct(address) as id, 'Address' as type from {0}
union 
select distinct(email_addr) as id, 'Email' as type from {0}
union 
select distinct(phone_number) as id, 'Phone' as type from {0}
'''.format(config['db_entities']))

aml_identity_g = GraphFrame(identity_nodes, identity_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src = "https://github.com/stephanieamrivera/upgraded-octo-parakeet/blob/main/slides/AML%20Example%20Graph%20Degrees.png?raw=true" width=850>

# COMMAND ----------

# DBTITLE 1,グラフプロパティを使用して頂点の次数を頂点プロパティとして追加し、次数が1の非人物ノードを除去する
from pyspark.sql.functions import *
import uuid
sc.setCheckpointDir('{}/chk_{}'.format(temp_directory, uuid.uuid4().hex))
result = aml_identity_g.degrees
result = aml_identity_g.vertices.join(result,'id')
identity_nodes2notpeople = result.filter(col("type") != 'Person').filter(col("degree") != 1)
identity_nodes2people = result.filter(col("type") == 'Person')
identity_nodes2 = identity_nodes2notpeople.union(identity_nodes2people)
display(identity_nodes2)


# COMMAND ----------

# DBTITLE 1,新しいグラフの構築
aml_identity_g2 = GraphFrame(identity_nodes2, identity_edges)

# COMMAND ----------

# DBTITLE 1,グラフアルゴリズムを使用してエンティティ間の関係を把握する
# MAGIC %md
# MAGIC [連結コンポーネント](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#connected-components)のようなグラフ組み込みモデルは、調査全体を大幅に簡素化します。連結エンティティを再帰的にデータセット結合する代わりに、この単純なAPI呼び出しで、少なくとも1つの共通エンティティを持つノードのグループを返します。

# COMMAND ----------

import uuid
sc.setCheckpointDir('{}/chk_{}'.format(temp_directory, uuid.uuid4().hex))
result = aml_identity_g2.connectedComponents()
result.select("id", "component", 'type').createOrReplaceTempView("components")

# COMMAND ----------

# DBTITLE 1,コンポーネントは少なくとも1つの共通エンティティを持つノードのグループ
# MAGIC %sql
# MAGIC SELECT * FROM components

# COMMAND ----------

# DBTITLE 1,複数の「人物」エンティティを持つコンポーネントを選択する
# MAGIC %md
# MAGIC グラフ構造についての深い洞察を得るにつれて、その結果を標準SQLでさらに分析できます。シルバーレイヤーとして使用することで、このデータアセットを最小限のコストで合成IDの検出に活用できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view ptntl_synthetic_ids
# MAGIC as
# MAGIC with dupes as
# MAGIC (
# MAGIC   select 
# MAGIC     component, 
# MAGIC     count(case when type = 'Person' then 1 end) person_ct 
# MAGIC   from components
# MAGIC   group by component
# MAGIC   having person_ct > 1
# MAGIC )
# MAGIC select * from components
# MAGIC where component in (select component from dupes);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ptntl_synthetic_ids

# COMMAND ----------

# DBTITLE 1,例示グラフは全体グラフ内の連結コンポーネントの例でもある - 当該コンポーネントを表示
# MAGIC %sql
# MAGIC with example as (select component from ptntl_synthetic_ids WHERE id = "4960")
# MAGIC select * from ptntl_synthetic_ids WHERE component in (select * from example)

# COMMAND ----------

# MAGIC %md
# MAGIC 共有属性を明らかにすることで、調査を容易に進めることができます。

# COMMAND ----------

# DBTITLE 1,不審な人物/IDとその共有属性へのフィルタリング
suspicious_component_id = (
  spark
    .sql("select id as id0, component, type from ptntl_synthetic_ids")
    .filter(col('type') == 'Person')
    .drop('type')
)

ids = suspicious_component_id.join(spark.table("ptntl_synthetic_ids"), ['component']).filter(col('id0') != col('id'))
ids.createOrReplaceTempView("sus_ids")

# COMMAND ----------

# DBTITLE 1,例示に戻る
# MAGIC %sql
# MAGIC select * from sus_ids WHERE component = "68719476738"

# COMMAND ----------

# DBTITLE 1,合成スコアは共有属性の数と共有する人物の数の合計
# MAGIC %sql 
# MAGIC CREATE OR REPLACE table entity_synth_scores as (
# MAGIC   select
# MAGIC     component,
# MAGIC     id0,
# MAGIC     count(*) as synth_score
# MAGIC   from
# MAGIC     sus_ids
# MAGIC   GROUP BY
# MAGIC     component,
# MAGIC     id0
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC このクエリの結果から、1つの一致属性（住所など）のみで構成されるコホートはそれほど問題ではないと考えられます。しかし、一致する属性が増えるほど、アラートを出すべきと判断されます。以下に示すように、3つの属性すべてが一致するケースをフラグ立てることで、SQLアナリストが毎日全エンティティに対して実行されるグラフ分析の結果を得られるようになります。

# COMMAND ----------

# DBTITLE 1,スコアが高いほどリスクが高い
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * from entity_synth_scores

# COMMAND ----------

entity_synth_scores = sql("""SELECT * from entity_synth_scores""")
entity_synth_scores.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable(config['db_synth_scores'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ストラクチャリング / スマーフィング
# MAGIC 
# MAGIC よく見られるもう一つのパターンが「ストラクチャリング」と呼ばれるものです。複数のエンティティが共謀して、複数の銀行へ少額の「レーダーをかいくぐる」送金を行い、その後それらの銀行が最終機関へより大きな集計額を送金するというものです。このシナリオでは、当局が通常フラグを立てる10,000ドルの閾値をすべての当事者が下回っています。これはグラフ分析で容易に実現できるだけでなく、使用するモチーフ検索技法を他のネットワーク構成に拡張して同様の方法で他のアラートを検出できるよう自動化することも可能です。この技法を下記の簡単なグラフで表現します。
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/AML-on-Lakehouse-Platform-blog-img-4.jpg" width="800"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 前述のとおり、このようなパターンを見つけることを目的としたネットワーク構造を容易に構築できます。

# COMMAND ----------

# DBTITLE 1,GraphFrameの作成
entity_edges = spark.sql(
"""
select 
  originator_id as src, 
  beneficiary_id as dst, 
  txn_amount, txn_id as id 
from {0}
""".format(config['db_transactions'])
)

entity_nodes = spark.sql(
"""
select 
  distinct(A.id), 
  'entity' as type 
from
  (
    select 
      distinct(originator_id) as id 
    from {0}
    union 
    select 
      distinct(beneficiary_id) as id 
    from {0}
  ) A
""".format(config['db_transactions'])
)

aml_entity_g = GraphFrame(entity_nodes, entity_edges)
entity_edges.createOrReplaceTempView("entity_edges")
entity_nodes.createOrReplaceTempView("entity_nodes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### モチーフ
# MAGIC 
# MAGIC 考えられるシナリオを検出するための基本的なモチーフ検索コードを記述します。
# MAGIC 
# MAGIC <img src="https://github.com/SpyderRivera/upgraded-octo-parakeet/blob/main/slides/motif.png?raw=true" width="800"/>

# COMMAND ----------

# DBTITLE 1,このモチーフは存在する
motif = "(a)-[e1]->(b); (b)-[e2]->(c); (d)-[e3]->(f); (f)-[e5]->(c); (c)-[e6]->(g)"
struct_scn_1 = aml_entity_g.find(motif)

display(struct_scn_1)

# COMMAND ----------

# DBTITLE 1,gへの送金額が大きい場合にgでサブグラフを結合する
joined_graphs = (
  struct_scn_1.alias("graph1")
  .join(struct_scn_1.alias("graph2"), col("graph1.g.id") == col("graph2.g.id"))
  .filter(col("graph1.e6.txn_amount") + col("graph2.e6.txn_amount") > 10000)
)

joined_graphs.selectExpr("graph1.*").write.option("overwriteSchema", "true").mode('overwrite').saveAsTable(config['db_structuring'])

# COMMAND ----------

# MAGIC %md
# MAGIC モチーフパターンから抽出されたように、グラフメタデータを構造化データセットと結合すると、上記で検出された正確なシナリオが以下に示されます。

# COMMAND ----------

levels = sql(
    """
    SELECT * FROM (SELECT DISTINCT entity0.name l0_name, entity1.name l1_name, entity2.name l2_name, entity3.name l3_name
    from {0} graph
    join {1} entity0
    on graph.a.id = entity0.entity_id
    join {1} entity1
    on graph.b.id = entity1.entity_id
    join {1} entity2 
    on graph.c.id = entity2.entity_id
    join {1} entity3
    on graph.g.id = entity3.entity_id
    where entity3.entity_type = 'Company') abcg
    UNION ALL
    SELECT * FROM (SELECT DISTINCT entity0.name l0_name, entity1.name l1_name, entity2.name l2_name, entity3.name l3_name
    from {0} graph
    join {1} entity0
    on graph.d.id = entity0.entity_id
    join {1} entity1
    on graph.f.id = entity1.entity_id
    join {1} entity2 
    on graph.c.id = entity2.entity_id
    join {1} entity3
    on graph.g.id = entity3.entity_id
    where entity3.entity_type = 'Company') dfcg
    """.format(config['db_structuring'], config['db_entities'])
  )
levels.write.option("overwriteSchema", "true").mode('overwrite').saveAsTable(config['db_structuring_levels'])

# COMMAND ----------

display(levels)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ラウンドトリッピング
# MAGIC 
# MAGIC この資金フローパターンにはいくつかのバリエーションがありますが、基本的な前提は送金元と送金先が同一であるということです。前述の「ストラクチャリング」シナリオと同様に、簡単なモチーフ検索でこのようなパターンを発見できます。
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/aml/RoudTrip.png" width="650"/>

# COMMAND ----------

# DBTITLE 1,類似したラウンドトリッピングモチーフ
motif = "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)"
round_trip = aml_entity_g.find(motif)
round_trip.write.mode('overwrite').saveAsTable(config['db_roundtrips'])
display(round_trip)

# COMMAND ----------

# MAGIC %md
# MAGIC この問題をグラフとして扱うことで、ラウンドトリップAMLパターンに関与するすべての当事者と集計金額を一緒に記録できます。

# COMMAND ----------

display(
  sql(
    """
    select
      ents0.name original_entity,
      ents1.name intermediate_entity_1,
      ents2.name intermediate_entity_2,
      ents3.name intermediate_entity_3,
      int(rt.e1.txn_amount) + int(rt.e2.txn_amount) + int(rt.e3.txn_amount) + int(rt.e4.txn_amount) agg_txn_amount
    from
      {0} rt
      join {1} ents0 on rt.a.id = ents0.entity_id
      join {1} ents1 on rt.b.id = ents1.entity_id
      join {1} ents2 on rt.c.id = ents2.entity_id
      join {1} ents3 on rt.d.id = ents3.entity_id
    """.format(config['db_roundtrips'], config['db_entities'])
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## リスクスコア伝播
# MAGIC 
# MAGIC 4つ目のパターンは、この問題が単純なSQL文では対処できない理由の完璧な例です。特定された高リスクエンティティ（政治的に公開されている人物など）はその周辺に影響（ネットワーク効果）を及ぼします。彼らと交流するすべてのエンティティのリスクスコアは、影響圏を反映するよう調整する必要があります。反復的なアプローチを使用することで、トランザクションのフローを任意の深さまで追跡し、ネットワーク内の影響を受ける他者のリスクスコアを調整できます。幸いなことに、[Pregel API](https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api) はまさにその目的のために構築されています。
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/aml/pregel.png" width="900"/>

# COMMAND ----------

entity_edges = spark.sql("""
select 
  originator_id as src, 
  beneficiary_id as dst, 
  txn_amount, 
  txn_id as id 
from {}
""".format(config['db_transactions']))

entity_nodes = spark.sql("""
select 
  distinct(A.id), risk 
from
  (
    select 
      distinct(entity_id) as id, 
      risk_score risk 
    from {}
  ) A
""".format(config['db_entities']))

entity_edges.createOrReplaceTempView("entity_edges")
entity_nodes.createOrReplaceTempView("entity_nodes")
aml_entity_g = GraphFrame(entity_nodes, entity_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC Pregelは関数とメッセージのセットに対して動作します。各ノードはその隣接ノードに情報を伝播できます。各隣接ノードは状態を更新し、それ以上メッセージを送信できなくなるか最大反復回数に達するまで、メッセージをダウンストリームに伝播します。以下の例では、最大3層の深さを分析対象とし、リスクスコアを反復的に集計します。

# COMMAND ----------

from graphframes import GraphFrame
from pyspark.sql.functions import coalesce, col, lit, sum, when, greatest
from graphframes.lib import Pregel

ranks = aml_entity_g.pregel \
     .setMaxIter(3) \
     .withVertexColumn("risk_score", col("risk"), coalesce(Pregel.msg()+ col("risk"), col("risk_score"))) \
     .sendMsgToDst(Pregel.src("risk_score")/2 )  \
     .aggMsgs(sum(Pregel.msg())) \
     .run()

ranks.write.mode('overwrite').saveAsTable(config['db_risk_propagation'])

# COMMAND ----------

display(
  sql(
    """
    select
      a.id,
      a.risk_score,
      a.risk original_risk_score,
      b.name
    from
      {0} a
      join {1} b on a.id = b.entity_id
    where
      id >= 10000001
    """.format(config['db_risk_propagation'], config['db_entities'])
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC このノートブックでは、AML活動に関するより深い洞察を得るためのネットワーク分析の概念を丁寧に紹介しました。個々のトランザクションを単独で調査するのではなく、トランザクションパターン周辺のより多くのコンテキストを取得する必要性を示しました。グラフ理論の有用性を示した一方で、より高いドメイン専門知識を持ちながらもソフトウェアエンジニアリングの経験が少ない調査チームにネットワーク分析の活用を民主化するため、調査側面を標準SQLプロセスに委ねることを選択しました。新しいトランザクションが発見されるたびにこれらのデータアセットを構築することで、エンジニアやデータサイエンティストはアナリストが活用できるシンプルなダッシュボードを構築できます。
