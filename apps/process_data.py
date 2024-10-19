from pyspark.sql import SparkSession

hadoop_aws_jar = "/opt/jars/hadoop-aws-3.2.4.jar"
aws_sdk_jar = "/opt/jars/aws-java-sdk-bundle-1.11.901.jar"

print("spart start")

spark = SparkSession.builder \
    .appName("Read MinIO File") \
    .master('spark://spark-master:7077')\
    .config('spark.jars',f'{hadoop_aws_jar},{aws_sdk_jar}')\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "htr93AeV4AyoNvdyrjRM") \
    .config("spark.hadoop.fs.s3a.secret.key", "bluVvDq5TYyWo3oHd4s8M6jHsPIdjC72Y3FvwS3J") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("create app")

print('reading')
df = spark.read.csv('s3a://reddit/dataengineering/raw/reddit_dataengineering_20241019.csv',header=True,inferSchema=True)

# print(len(df))
df.show(3)

spark.stop()