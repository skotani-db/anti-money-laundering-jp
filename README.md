<img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fs-lakehouse-logo.png width="600px">

[![DBU](https://img.shields.io/badge/DBU-L-yellow)]()
[![COMPLEXITY](https://img.shields.io/badge/COMPLEXITY-201-yellow)]()

*マネーロンダリング対策（AML）コンプライアンスは、金融機関の監督を目的として、米国およびグローバル全体で最重要規制課題の一つとなっています。デジタルバンキングへの移行が進む中、金融機関は毎日数十億件のトランザクションを処理しており、より厳格な支払い監視や強固な顧客確認（KYC）ソリューションが導入されているにもかかわらず、マネーロンダリングのリスクは日々拡大しています。本ソリューションでは、FSI（金融サービス業界）がLakehouseプラットフォーム上でエンタープライズ規模のAMLソリューションを構築する方法について、顧客との協業経験を共有します。このソリューションは、強力な監視機能を提供するだけでなく、現代のオンラインマネーロンダリング脅威のリアルに対応するための革新的な拡張・適応機能も備えています。グラフ分析、自然言語処理（NLP）、コンピュータービジョンを通じて、データとAIの世界におけるAML防止の多様な側面を明らかにします。*

---
<anindita.mahapatra@databricks.com>, <ricardo.portilla@databricks.com>, <sri.ghattamaneni@databricks.com>

<img src='https://databricks.com/wp-content/uploads/2021/07/aml-blog-img-1-a.png' width=800>

&copy; 2021 Databricks, Inc. All rights reserved. このノートブックのソースコードは、Databricksライセンス [https://databricks.com/db-license-source] に基づいて提供されています。含まれているまたは参照されているすべてのサードパーティライブラリは、以下に記載するライセンスに従います。

| ライブラリ                              | 説明                    | ライセンス  | ソース                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| graphframes:graphframes                | グラフライブラリ         | Apache2    | https://github.com/graphframes/graphframes          |
| torch                                  | Pytorchライブラリ        | BSD        | https://pytorch.org/                                |
| Pillow                                 | 画像処理                 | HPND       | https://python-pillow.org/                          |
| Splink                                 | エンティティ結合         | MIT        | https://github.com/moj-analytical-services/splink   |

このアクセラレーターを実行するには、このリポジトリをDatabricksワークスペースにクローンしてください。DBR 11.0以降のランタイムを実行している任意のクラスターにRUNMEノートブックをアタッチし、「Run-All」でノートブックを実行してください。アクセラレーターパイプラインを記述するマルチステップジョブが作成され、そのリンクが表示されます。マルチステップジョブを実行して、パイプラインの動作を確認してください。

ジョブ設定はRUNMEノートブックにJSON形式で記載されています。アクセラレーターの実行に伴うコストはユーザーの責任となります。
