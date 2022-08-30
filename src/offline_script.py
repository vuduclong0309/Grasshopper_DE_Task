# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:39:58.058207Z","iopub.execute_input":"2022-08-30T10:39:58.058682Z","iopub.status.idle":"2022-08-30T10:39:58.070696Z","shell.execute_reply.started":"2022-08-30T10:39:58.058627Z","shell.execute_reply":"2022-08-30T10:39:58.068777Z"}}
# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

import os
for dirname, _, filenames in os.walk('/kaggle/input'):
    for filename in filenames:
        print(os.path.join(dirname, filename))

# You can write up to 20GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All"
# You can also write temporary files to /kaggle/temp/, but they won't be saved outside of the current session

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:39:58.073600Z","iopub.execute_input":"2022-08-30T10:39:58.074482Z","iopub.status.idle":"2022-08-30T10:40:11.547039Z","shell.execute_reply.started":"2022-08-30T10:39:58.074420Z","shell.execute_reply":"2022-08-30T10:40:11.544647Z"}}
#!pip install pyspark

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:11.552630Z","iopub.execute_input":"2022-08-30T10:40:11.553949Z","iopub.status.idle":"2022-08-30T10:40:11.565630Z","shell.execute_reply.started":"2022-08-30T10:40:11.553868Z","shell.execute_reply":"2022-08-30T10:40:11.564195Z"}}
import os
import pandas as pd
import numpy as np

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:47:31.062408Z","iopub.execute_input":"2022-08-30T10:47:31.063693Z","iopub.status.idle":"2022-08-30T10:47:31.071486Z","shell.execute_reply.started":"2022-08-30T10:47:31.063597Z","shell.execute_reply":"2022-08-30T10:47:31.070039Z"}}
#input / output path
l3_path = '/kaggle/input/exchangesimul/l3_data_v3.1.csv'
expect_l1_path = '/kaggle/input/exchangesimul/expected_l1_data_v3.1.csv'
output_path = 'output/l3_data'

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:11.582792Z","iopub.execute_input":"2022-08-30T10:40:11.583921Z","iopub.status.idle":"2022-08-30T10:40:11.602887Z","shell.execute_reply.started":"2022-08-30T10:40:11.583860Z","shell.execute_reply":"2022-08-30T10:40:11.601379Z"}}
#spark setup
spark = SparkSession.builder.master("local[*]").appName("L1Generator").getOrCreate()
spark

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:11.604450Z","iopub.execute_input":"2022-08-30T10:40:11.605582Z","iopub.status.idle":"2022-08-30T10:40:12.190817Z","shell.execute_reply.started":"2022-08-30T10:40:11.605542Z","shell.execute_reply":"2022-08-30T10:40:12.189358Z"}}
raw_ord_book = spark.read.csv(l3_path, header = True).orderBy('time')
#raw_ord_book.show()

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:12.192469Z","iopub.execute_input":"2022-08-30T10:40:12.194467Z","iopub.status.idle":"2022-08-30T10:40:12.215633Z","shell.execute_reply.started":"2022-08-30T10:40:12.194401Z","shell.execute_reply":"2022-08-30T10:40:12.214282Z"}}
raw_ord_book.createOrReplaceTempView("l1_order_book")
# raw_ord_book.printSchema()

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:12.217600Z","iopub.execute_input":"2022-08-30T10:40:12.218426Z","iopub.status.idle":"2022-08-30T10:40:12.652256Z","shell.execute_reply.started":"2022-08-30T10:40:12.218377Z","shell.execute_reply":"2022-08-30T10:40:12.650873Z"}}
#spark.sql("""SELECT * from l1_order_book limit 5""").show()

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:12.653595Z","iopub.execute_input":"2022-08-30T10:40:12.654610Z","iopub.status.idle":"2022-08-30T10:40:13.787944Z","shell.execute_reply.started":"2022-08-30T10:40:12.654562Z","shell.execute_reply":"2022-08-30T10:40:13.786395Z"}}
#spark.sql("""
#    SELECT
#                *,
#                row_number() OVER(PARTITION BY delete_order_id ORDER BY time ASC) AS del_rank
#            FROM l1_order_book
#""").orderBy('seq_num').show()

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:13.789917Z","iopub.execute_input":"2022-08-30T10:40:13.790564Z","iopub.status.idle":"2022-08-30T10:40:13.855547Z","shell.execute_reply.started":"2022-08-30T10:40:13.790508Z","shell.execute_reply":"2022-08-30T10:40:13.854189Z"}}
"""
    This ETL step purpose is to normalize all order type to SELL / BUY order for calculation simplification
    - For delete order we simply assume as corresponding SELL / BUY order with negative qty as opposed to its counterpart
    - I didn't understand trade order, and as I explore there is only 4 transaction with this type, I will skip or I can clarify later
        - Once clarified, can chain 1 more sql e.g trade_order_normalization_sql to take care of trade order.
"""
delete_order_normalization_sql = """
    SELECT
        ord_ori.seq_num,
        ord_ori.time,
        COALESCE(ord_ori.add_side, ord_ori.delete_side) AS add_side, -- If there are more type of order, use CASE WHEN instead
        COALESCE(ord_ori.add_price, ord_del.del_add_price) AS add_price,
        COALESCE(ord_ori.add_qty, CAST(-ord_del.del_add_qty AS INT)) AS add_qty
    FROM
        (
            SELECT
                *,
                row_number() OVER(PARTITION BY delete_order_id ORDER BY seq_num ASC) AS del_rank
            FROM l1_order_book
        ) AS ord_ori
        LEFT JOIN (
            SELECT
                row_number() OVER(PARTITION BY add_order_id ORDER BY seq_num ASC) AS add_rank,
                seq_num,
                add_order_id,
                add_price AS del_add_price,
                add_qty AS del_add_qty
            FROM l1_order_book
        ) AS ord_del
        ON ord_ori.delete_order_id = ord_del.add_order_id AND ord_ori.del_rank = ord_del.add_rank
"""

nml_ord_book = spark.sql(delete_order_normalization_sql).orderBy('seq_num').cache()

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:13.857580Z","iopub.execute_input":"2022-08-30T10:40:13.858076Z","iopub.status.idle":"2022-08-30T10:40:13.865667Z","shell.execute_reply.started":"2022-08-30T10:40:13.858028Z","shell.execute_reply":"2022-08-30T10:40:13.862552Z"}}
#nml_ord_book.unpersist()

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:13.867170Z","iopub.execute_input":"2022-08-30T10:40:13.884915Z","iopub.status.idle":"2022-08-30T10:40:13.894840Z","shell.execute_reply.started":"2022-08-30T10:40:13.884840Z","shell.execute_reply":"2022-08-30T10:40:13.893172Z"}}
# Create Schema for output csv
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('time', StringType(), True),
  StructField('bid_price', DoubleType(), True),
  StructField('ask_price', DoubleType(), True),
  StructField('bid_size', IntegerType(), True),
  StructField('ask_size', IntegerType(), True),
  StructField('seq_num', StringType(), True),
  ])

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:40:13.896205Z","iopub.execute_input":"2022-08-30T10:40:13.896672Z","iopub.status.idle":"2022-08-30T10:40:16.095669Z","shell.execute_reply.started":"2022-08-30T10:40:13.896630Z","shell.execute_reply":"2022-08-30T10:40:16.093654Z"}}
# Main generation logic after order are normalized
bids = {}
asks = {}
ans = []

def generateEntry(seq_num, time):
    if asks == {} or bids == {}:
        return
    minAsk = min(asks.keys())
    maxBid = max(bids.keys())
    ans_ent = [time, maxBid, minAsk, bids[maxBid], asks[minAsk], seq_num]
    if ans:
        if ans_ent[1:5] == ans[-1][1:5]:
            return
    ans.append(ans_ent)

def updateBook(entry):
    if(entry['add_side'] == 'BUY'):
        #print(entry)
        add_price = float(entry['add_price'])
        bids[add_price] = bids.get(add_price, 0) + int(entry['add_qty'])
        if bids[add_price] == 0:
            del bids[add_price]

    elif (entry['add_side'] == 'SELL'):
        #print(entry)
        add_price = float(entry['add_price'])
        asks[add_price] = asks.get(add_price, 0) + int(entry['add_qty'])
        if asks[add_price] == 0:
            del asks[add_price]
    generateEntry(entry['seq_num'], entry['time'])

for entry in nml_ord_book.rdd.collect():
    updateBook(entry)

res_df = spark.createDataFrame(ans, schema).orderBy('seq_num').cache()

# %% [code] {"execution":{"iopub.status.busy":"2022-08-30T10:47:41.041745Z","iopub.execute_input":"2022-08-30T10:47:41.042241Z","iopub.status.idle":"2022-08-30T10:47:42.026391Z","shell.execute_reply.started":"2022-08-30T10:47:41.042204Z","shell.execute_reply":"2022-08-30T10:47:42.024774Z"}}
# Write Output
if os.path.exists(output_path):
    os.system("rm -rf " + output_path)

# unify back to 1 file, else there will be multiple csv
res_df.repartition(1).write.csv(output_path)

# %% [code]
