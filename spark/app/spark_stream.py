import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy


logging.basicConfig(level=logging.INFO)


# -----------------------------
# Cassandra (DDL only)
# -----------------------------
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully")


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
        );
    """)
    logging.info("Table created successfully")


def create_cassandra_connection():
    """
    Connect to Cassandra ONLY for DDL (keyspace/table creation).
    Data writes are handled by the Spark Cassandra Connector.
    """
    try:
        cluster = Cluster(
            ['cassandra'],
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=5  # Cassandra 4.x → protocol v5
        )
        cass_session = cluster.connect()
        logging.info("Connected to Cassandra for DDL")
        return cass_session
    except Exception as e:
        logging.error(f"Cassandra connection error: {e}")
        return None


# -----------------------------
# Spark Session
# -----------------------------
def create_spark_connection():
    """Create SparkSession configured for Docker + Kafka + Cassandra."""
    try:
        spark = (
            SparkSession.builder
            .appName("SparkDataStreaming")
            # master is usually set via spark-submit: --master spark://spark-master:7077
            # .config("spark.master", "spark://spark-master:7077")

            # Cassandra connector
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.cassandra.connection.port", "9042")

            # Executor stability (important in Docker)
            .config("spark.executor.memory", "512m")
            .config("spark.executor.memoryOverhead", "256m")
            .config("spark.executor.cores", "1")
            .config("spark.executor.instances", "1")
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "100s")


            # Cassandra write tuning
            .config("spark.cassandra.output.concurrent.writes", "2")
            .config("spark.cassandra.output.throughputMBPerSec", "10")

            # We already put jars into the image, don't auto-download
            .config("spark.jars.packages", "")
            .config("spark.sql.shuffle.partitions", "4")

            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        logging.info("Spark connection established successfully")
        return spark

    except Exception as e:
        logging.error(f"Spark Session connection error: {e}")
        return None


# -----------------------------
# Kafka Source
# -----------------------------
def connect_to_kafka(spark_conn):
    """Create a streaming DataFrame from Kafka topic."""
    try:
        spark_df = (
            spark_conn.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 2000)  # safer micro-batch size
            .load()
        )

        logging.info("Kafka DataFrame connected successfully")
        return spark_df

    except Exception as e:
        logging.warning(f"Couldn't connect to Kafka to retrieve DataFrame: {e}")
        return None


# -----------------------------
# Transform JSON → Columns
# -----------------------------
def create_selection_df_from_kafka(spark_df):
    """Parse JSON from Kafka 'value' into structured columns."""
    schema = StructType([
        StructField("id", StringType(), False),   # keep as STRING for Spark
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

    selection_df = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # DO NOT cast to UUID in Spark — Spark doesn't support UUIDType.
    # The Cassandra connector can convert valid UUID strings to Cassandra UUID.
    # selection_df = selection_df.withColumn("id", col("id").cast("uuid"))

    logging.info("Transformed Kafka JSON into structured DataFrame")
    return selection_df


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is None:
            logging.error("Kafka stream is None. Exiting.")
            raise SystemExit(1)

        selection_df = create_selection_df_from_kafka(spark_df)

        # Use Python Cassandra driver ONLY for schema creation
        session = create_cassandra_connection()
        if session is None:
            logging.error("Could not create Cassandra keyspace/table. Exiting.")
            raise SystemExit(1)

        create_keyspace(session)
        create_table(session)

        logging.info("Starting streaming query to Cassandra...")

        streaming_query = (
            selection_df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("keyspace", "spark_stream")
            .option("table", "created_users")
            .outputMode("append")
            .trigger(processingTime="5 seconds")
            .start()
        )

        streaming_query.awaitTermination()
