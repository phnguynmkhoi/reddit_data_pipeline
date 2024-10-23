from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,IntegerType,BooleanType,StringType,TimestampType,FloatType
from pyspark.sql.functions import col,from_unixtime
import datetime

hadoop_aws_jar = "/opt/spark/jars/hadoop-aws-3.2.0.jar"
aws_sdk_jar = "/opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar"

print("spart start")

spark = SparkSession.builder \
    .appName("Read MinIO File") \
    .master('spark://spark-master:7077')\
    .config('spark.jars',f'{hadoop_aws_jar},{aws_sdk_jar}')\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "2irsHaALo6EzOxenTMeu") \
    .config("spark.hadoop.fs.s3a.secret.key", "bzPNS4HOyAj9sxnTO9va4qnoz6VGdxRfP4LH7QbO") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("create app")

schema = StructType([
    StructField('id',StringType(),False),
    StructField('title',StringType(),False),
    StructField('author',StringType(),False),
    StructField('num_comments',IntegerType(),False),
    StructField('upvote_ratio',FloatType(),False),
    StructField('score',FloatType(),False),
    StructField('created_utc',StringType(),False),
    StructField('over_18',BooleanType(),False),
    StructField('url',StringType(),False),
    StructField('subreddit',StringType(),False)
])

today = datetime.datetime.now()
today = today.strftime('%Y%m%d')
path = f's3a://reddit/transformed/{today}'
csv_path = f's3a://reddit/raw/{today}/reddit_dataengineering_{today}.csv'
df = spark.read.parquet(path)
# df = spark.read.option('delimiter','\t').csv(path,header=True,schema=schema)
df = df.withColumn("created_utc", from_unixtime(col("created_utc")))

df.filter(col('subreddit')=='vietnam').show(5)

# try:
#     df.write.mode('append').parquet(f's3a://reddit/transformed/20241019')
# except Exception as e:
#     print(e)