from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("Sample_Spark")\
        .config('spark.jars','/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.6.0.jar')\
        .getOrCreate()

df=spark.read.format("csv").option("header","true").load("/tmp/posts.csv")


#only selecting 3 columns
df= df.select(f.col('id'),f.col('title'),f.col('body'))

df.write.parquet("/home/user/airflow/Airflow_output/posts.parquet", 
                 mode="overwrite")
df.show()

db_url = "jdbc:postgresql://localhost:5432/airflow"
table_name = "posts_spark"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

df.write.mode("overwrite").jdbc(url=db_url, table=table_name, properties=properties)

# df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/airflow',
#                                 driver='org.postgresql.Driver',
#                                 dbtable= 'posts_spark',
#                                 user='postgres',
#                                 password='postgres'
#                                 ).mode('overwrite')