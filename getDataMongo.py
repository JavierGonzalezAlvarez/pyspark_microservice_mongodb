import datetime
import pytz
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, TimestampType
from pyspark import SparkConf
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import json

print(pyspark.__version__)


def to_utc_ms(date, tz='America/New_York'):
    dt = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
    dt = pytz.timezone(tz).localize(dt)
    return int(dt.timestamp() * 1000.0)


def send_jira(dfData):
    tz = pytz.timezone('America/New_York')
    metric = dfData.collect()[0][1]
    description = dfData.collect()[0][2]
    date = dfData.collect()[0][4]
    dateFormatted = date.astimezone(tz)
    dateToString = dateFormatted.strftime('%Y-%m-%d %H:%M:%S')
    url = "https://client.atlassian.net/rest/api/2/issue"
    auth = HTTPBasicAuth("email@gmail.com", "pass")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = json.dumps({
        "fields": {
            "project":
            {
                "key": "ZXC"
            },
            "summary": metric,
            "description": description+" in the period "+dateToString,
            "issuetype": {
                "id": "10004"
            },
            "reporter": {"id": "c14b00bdd900688818"},
            "assignee": {"id": "b9c3ef5b460071162b"},
            "priority":  {"id": "2"},
        }
    })
    print(payload)
    response = requests.request(
        "POST",
        url,
        data=payload,
        headers=headers,
        auth=auth
    )
    print(response.content)


last_count = -1
client1 = ("database1", "collection1")
client2 = ("database2", "collection2")

input_uri = "mongodb://127.0.0.1:27017/database1.collection1"
output_uri = "mongodb://127.0.0.1:27017/database1.collection1"

spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("Pyspark") \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

schema = StructType([StructField("Metric", StringType(), True),
                     StructField("Description", StringType(), True), StructField(
                         "Count", LongType(), True),
                     StructField("Date", TimestampType(), True), ])

dfGeneral = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


def count_interrupted(lista):
    cnt = 0
    for i in range(len(lista) - 1):
        if lista[i + 1] != lista[i] + 1:
            cnt = cnt + 1
    return cnt


def check(start, end):
    global last_count, dfGeneral
    query1 = [{'$match': {'TXN_TIMESTAMP': {'$gte': start, '$lt': end}}},
              {'$group': {'_id': "None", 'conteo': {'$count': {}}}}]
    query2 = [{'$match': {'time': {'$gte': start, '$lt': end}}},
              {'$group': {'_id': "None", 'conteo': {'$count': {}}}}]

    try:

        collection1 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option("uri", input_uri) \
            .option("database", client1[0]) \
            .option("collection", client1[1]) \
            .option("pipeline", query1) \
            .load()

        collection2 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option("uri", input_uri) \
            .option("database", client2[0]) \
            .option("collection", client2[1]) \
            .option("pipeline", query2).load()

        collection1.printSchema()
        collection1.show()
        collection2.printSchema()
        collection2.show()

        if last_count >= 0:
            count = last_count - int(collection2.collect()[0][1])
            if count > 0:
                data = [("Trx in table",
                        f"Number of trx do not match : {count}", count, datetime.utcnow())]
                dfData = spark.createDataFrame(
                    data=data,
                    schema=schema
                )
                dfGeneral = dfGeneral.unionByName(dfData)
                dfData.printSchema()
                dfData.select(F.col("Date")).show()
                dfData.select(F.col("Description")).show()
                send_jira(dfData)

        last_count = int(collection1.collect()[0][1])
        spark.sql(f"SELECT {last_count} as lastCount").createOrReplaceGlobalTempView(
            "last_count")

        print(last_count)
        collection1.unpersist()
        collection2.unpersist()
    except Exception:
        print("Could not query")


def check_database():
    now = datetime.now()
    minutes = now - timedelta(minutes=15)
    start = minutes.strftime('%Y-%m-%dT%H:%M:%S')
    end = now.strftime('%Y-%m-%dT%H:%M:%S')
    start_ms = to_utc_ms(start)
    end_ms = to_utc_ms(end)
    print(start_ms)
    print(end_ms)
    print("Checking sql...")
    check(start_ms, end_ms)


if __name__ == '__main__':
    check_database()
