import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster


def create_keyspace(session):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_stream
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
            """)

    print("Keyspace created successfully")

def create_table(session):
    session.execute("""
            CREATE TABLE IF NOT EXISTS spark_stream.created_users(
                id UUID PRIMARY KEY,
                full_name TEXT,
                gender TEXT,
                address TEXT,
                postcode TEXT,
                email TEXT,
                user_name TEXT,
                dob TEXT,
                registered_date TEXT,
                phone TEXT,
                image TEXT
            )
                """)
    print("Table created successfully")


def create_cassandra_connection():
    #Connecting to cassandra Cluster
    try:
        cluster = Cluster(['cassandra'])
        cass_session = cluster.connect()

        return cass_session
    except Exception as e:
        logging.error(f"Cassandra connection error: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("user_name", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("image", StringType(), False),
    ])

    selection_df = spark_df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col("value"), schema).alias("data")).select("data.*")
    print(selection_df)
    return selection_df

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
                  .format('kafka') \
                  .option('kafka.bootstrap.servers','broker:29092') \
                  .option('subscribe','users_created') \
                  .option('startingOffsets', 'earliest') \
                  .option("maxOffsetsPerTrigger", 5000) \
                  .load()

        logging.info("Kafka dataframe connected successfully")

    except Exception as e:
        logging.warning(f"Couldn't connect to Kafka to retrieve dataframe: {e}")

    return spark_df

def create_spark_connection():
    #Create Spark Session
    s_conn = None
    try:
        s_conn = (SparkSession.builder \
                .appName('SparkDataStreaming') \
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
                .config('spark.cassandra.connection.host', 'cassandra') \
                .config("spark.executor.memory", "2g") \
                .config("spark.executor.cores", "1") \
                .config("spark.executor.memoryOverhead", "512m") \
                .config("spark.executor.instances", "1") \
                .config("spark.network.timeout", "600s") \
                .config("spark.executor.heartbeatInterval", "20s") \
                .config("spark.cassandra.output.concurrent.writes", "2") \
                .config("spark.cassandra.output.throughputMBPerSec", "10") \
                .getOrCreate())
        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info('Spark Connection established successfully')

    except Exception as e:
        logging.error(f'Spark Session connection error :{e}')

    return s_conn

if __name__ == "__main__":
    #Create Spark Connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (
                            selection_df.writeStream
                            .format("org.apache.spark.sql.cassandra")
                            .option('checkpointLocation', '/tmp/checkpoint')
                            .option('keyspace', 'spark_stream')
                            .option('table', 'created_users')
                            .start())

            streaming_query.awaitTermination()


