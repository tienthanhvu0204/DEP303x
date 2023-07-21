from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#Tao SparkSession
spark = SparkSession \
            .builder \
            .master('local') \
            .appName('asm2_spark_job') \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1/airflow_asm2') \
            .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1/airflow_asm2') \
            .enableHiveSupport() \
            .getOrCreate()

#Tao schema doc du lieu tu cac collections
questions_schema = StructType([
                     StructField('Id', IntegerType()),
                     StructField('OwnerUserId', StringType()),
                     StructField('CreationDate', StringType()),
                     StructField('ClosedDate', StringType()),
                     StructField('Score', IntegerType()),
                     StructField('Title', StringType()),
                     StructField('Body', StringType())
                    ])

answers_schema = StructType([
                     StructField('Id', IntegerType()),
                     StructField('OwnerUserId', StringType()),
                     StructField('CreationDate', StringType()),
                     StructField('ParentId', IntegerType()),
                     StructField('Score', IntegerType()),
                     StructField('Body', StringType())
                    ])

#Doc du lieu tu mongodb
questions_coll_df = spark.read \
                    .format('com.mongodb.spark.sql.DefaultSource') \
                    .option("uri", "mongodb://127.0.0.1/airflow_asm2.Questions") \
                    .schema(questions_schema) \
                    .load()
#questions_df.show()

answers_coll_df = spark.read \
                    .format('com.mongodb.spark.sql.DefaultSource') \
                    .option("uri", "mongodb://127.0.0.1/airflow_asm2.Answers") \
                    .schema(answers_schema) \
                    .load()
#answers_df.show()

#doi ten cot Id cau tra loi truoc khi join
answers_coll_df = answers_coll_df.withColumnRenamed('Id', 'AnswerId')

#dieu kien join
join_cond = questions_coll_df.Id == answers_coll_df.ParentId

#tao result_df bang cach join va group data
result_df = questions_coll_df.join(answers_coll_df, join_cond, 'leftouter') \
                            .groupBy('Id') \
                            .agg(count('AnswerId').alias('Number of answers'))

#ghi du lieu ra file csv
result_df.coalesce(1).write \
        .format("csv") \
        .options(header = "true", delimiter = ",") \
        .mode('overwrite') \
        .save("./data_output/")