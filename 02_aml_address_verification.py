# Databricks notebook source
# MAGIC %md 
# MAGIC このノートブックシリーズは https://github.com/databricks-industry-solutions/anti-money-laundering でご覧いただけます。このソリューションアクセラレーターの詳細については、https://www.databricks.com/blog/2021/07/16/aml-solutions-at-scale-using-databricks-lakehouse-platform.html をご参照ください。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 住所検証
# MAGIC ここで簡単に触れておきたいパターンとして、テキストで記載された住所と実際のストリートビュー画像のマッチングがあります。ファイルに登録されているエンティティに紐付けられた住所を検証する必要があることはよくあります。しかし、住所の視覚的な確認、クリーニング、検証を行うことは、手間がかかり時間を要する手作業のプロセスになりがちです。幸いなことに、Lakehouseアーキテクチャにより、Python、PyTorchを使用した機械学習ランタイム、事前学習済みオープンソースモデルを活用してこれをすべて自動化できます。以下は人間の目から見た有効な住所の例です。

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/aml/aml_image_matching.png" width=600>

# COMMAND ----------

# MAGIC %run ./config/aml_config

# COMMAND ----------

from pyspark.sql.functions import * 

addresses = (
  spark
    .table(config['db_entities'])
    .filter(col("address").isNotNull())
    .withColumn("address", translate(translate(col("address"), ',', ''), ' ', '+'))
    .select('address')
    .toPandas()
)

addresses.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Google Maps APIの使用
# MAGIC ストリートマップAPIを使用することで、アナリストは調査目的で物件の写真に迅速にアクセスできます。リクエストレート制限（Google Maps API利用規約を参照）はありますが、一部の物件データへのアクセスはAML調査において非常に価値があります。
# MAGIC ノートブックに認証情報を安全に渡すため、[シークレットAPI](https://docs.databricks.com/security/secrets/index.html) を使用します。

# COMMAND ----------

goog_api_key = dbutils.secrets.get(scope="solution-accelerator-cicd", key="google-api")

# COMMAND ----------

import json
import time
import requests
import urllib.error
import urllib.parse
import urllib.request

for index, row in addresses[0:100].iterrows():
    url = f"https://maps.googleapis.com/maps/api/streetview?parameters&size=640x640&fov=50&location={row['address']}&key={goog_api_key}"
    req = requests.get(url)
    with open(f'{temp_directory}/img_{index}.jpg', 'wb') as file:
       file.write(req.content)
    req.close()                                                      

# COMMAND ----------

# MAGIC %md
# MAGIC matplotlibを使用することで、Databricksノートブックの中から特定の住所を確認できます。この特定の住所は、いかなる建物・物件も指していないようです。この追加コンテキストは、進行中の不審取引報告書（SAR）の一部として報告する必要があるかもしれません。

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
plt.figure(figsize=(10, 10))
img=mpimg.imread(f'{temp_directory}/img_0.jpg')
imgplot = plt.imshow(img)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 拡張知能
# MAGIC ランダムな写真を調査する複雑さに加えて、アナリストが手動で検査しなければならない膨大なデータ量が、効率的な調査プロセスの妨げになることが多いです。Googleマップから写真にプログラム的にアクセスし、物件・建物を自動的に検出するモデルをトレーニングできないでしょうか？AIは自動化されたブラックボックス型の意思決定エンジンと見なされることが多いですが、私たちはAMLの文脈でAIを、アナリストがより速く、より効果的にAML調査を実施するために必要なすべてのコンテキストを提供する拡張知能プロセスとして考えています。

# COMMAND ----------

# MAGIC %md
# MAGIC インフラ、スキルセット、データラベリングの手作業において高コストとなる独自のコンピュータービジョンモデルをトレーニングする代わりに、アナリストがPyTorchの[事前学習済み](http://pytorch.org/docs/master/torchvision/models.html)モデルを使用して予測する方法を示します。使用するモデルは[ImageNet](http://www.image-net.org/)データセットで学習した[VGG16](https://pytorch.org/hub/pytorch_vision_vgg/)畳み込みネットワークです。

# COMMAND ----------

import io
import numpy as np 

from PIL import Image
import requests
from matplotlib import cm

from torch.autograd import Variable
import torchvision.models as models
import torchvision.transforms as transforms

# VGGのトレーニング時に使用されたクラスラベルをJSON形式で取得（上記の「サンプルコード」リンクより）
LABELS_URL = 'https://raw.githubusercontent.com/raghakot/keras-vis/master/resources/imagenet_class_index.json'
response = requests.get(LABELS_URL)  # HTTPのGETリクエストを実行し、レスポンスを格納する
labels = {int(key): value for key, value in response.json().items()}
img_and_labels = {}

for i in range(100):
  
  img=mpimg.imread(f'{temp_directory}/img_' + str(i) + '.jpg')
  img = Image.fromarray(img)
  # 変換パイプラインを使用してこれらすべての前処理を実行できる
  min_img_size = 224  # PyTorchの事前学習済みモデルのドキュメントに記載されているように、最小サイズは224pxです
  transform_pipeline = transforms.Compose([transforms.Resize(min_img_size),
                                         transforms.ToTensor(),
                                         transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                                              std=[0.229, 0.224, 0.225])])
  img = transform_pipeline(img)

  # PyTorchの事前学習済みモデルはTensorのディメンションが(入力画像数, カラーチャンネル数, 高さ, 幅)であることを期待します
  # 現在は(カラーチャンネル数, 高さ, 幅)なので、新しい軸を挿入して修正します
  img = img.unsqueeze(0)  # インデックス0（他の軸/ディメンションの前）に新しい軸を挿入する

  # 画像の前処理が完了したら、Variableに変換する必要があります
  # PyTorchモデルは入力がVariableであることを期待します。PyTorch VariableはPyTorch Tensorのラッパーです
  img = Variable(img)

  # モデルをロードして予測を取得する
  vgg = models.vgg16(pretrained=True)  # 数分かかる場合があります
  prediction = vgg(img)  # (バッチ, クラスラベル数)のshapeのTensorを返す
  prediction = prediction.data.numpy().argmax()  # 最大値を持つクラスラベルのインデックスが予測結果になります
  img_and_labels[i] = labels[prediction]

# COMMAND ----------

# MAGIC %md
# MAGIC 以下に示すように、事前学習済みモデルはすでに多くの情報をすぐに提供しており、特定の住所の正当性を理解するのに非常に役立ちます。

# COMMAND ----------

img_and_labels

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lakeの機能により、非構造化データへの参照と、以下の分類結果での簡単なクエリのためのラベルを一緒に保存できます。

# COMMAND ----------

import pandas as pd 
pdf = pd.DataFrame.from_dict(img_and_labels, orient='index', columns=['image_number', 'label'])
spark.createDataFrame(pdf).filter(col("label") != 'envelope').write.mode('overwrite').saveAsTable(config['db_streetview'])

# COMMAND ----------

display(sql("select label, count(1) from {} group by label".format(config['db_streetview'])))

# COMMAND ----------

# MAGIC %md
# MAGIC ### まとめ
# MAGIC 画像分類はデータサイエンス・データエンジニアリングの作業になることが多いですが、結果をDeltaテーブルに保存することで、AMLアナリストがシンプルなダッシュボードやSQL機能を通じてケースをさらに調査できます。Delta Lakeの機能により、非構造化データへの参照とラベルを一緒に保存でき、以下の分類結果での簡単なクエリが可能です。Delta Lakeで非構造化データをクエリできる機能は、アナリストにとって膨大な時間の節約となり、検証プロセスを数日・数週間から数分に短縮します。
