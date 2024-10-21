import config
import logging
from time import sleep
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0"
    ]
)

kafka_security_options = {
    'kafka.bootstrap.servers': f'{config.KAFKA_URL}',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{config.KAFKA_USER}\" password=\"{config.KAFKA_PASSWORD}\";',
}

postgresql_settings_cloud = {
    'user': f'{config.POSTGRES_CLOUD_USER}',
    'password': f'{config.POSTGRES_CLOUD_PASSWORD}'
}

postgresql_settings_local = {
    'user': f'{config.POSTGRES_LOCAL_USER}',
    'password': f'{config.POSTGRES_LOCAL_PASSWORD}'
}

def spark_init(app_name) -> SparkSession:
    return (SparkSession.builder
            .master("local")
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate())

def read_restaurant_stream(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
        StructField("datetime_created", LongType())
    ])

    try:
        return (spark
                .readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', config.KAFKA_URL)
                .option("subscribe", config.TOPIC_NAME_REQ)
                .options(**kafka_security_options)
                .load()
                .withColumn('value', f.col('value').cast(StringType()))
                .withColumn('event', f.from_json(f.col('value'), schema))
                .selectExpr('event.*'))
    except Exception as e:
        logger.error(f"Error read from Kafka: {str(e)}")

def write_restaurant_stream(result_df: DataFrame):
    try:
        return (result_df
                .select(f.to_json(f.struct(
                    'restaurant_id',
                    'adv_campaign_id',
                    'adv_campaign_content',
                    'adv_campaign_owner',
                    'adv_campaign_owner_contact',
                    'adv_campaign_datetime_start',
                    'adv_campaign_datetime_end',
                    'datetime_created',
                    'client_id',
                    'trigger_datetime_created')).alias('value'))
                .write
                .mode("append")
                .format("kafka")
                .option('kafka.bootstrap.servers', config.KAFKA_URL)
                .options(**kafka_security_options)
                .option("topic", config.TOPIC_NAME_RESP)
                .option("checkpointLocation", "test_query")
                .save())
    except Exception as e:
        logger.error(f"Error write to Kafka: {str(e)}")

def filtered_read_stream(restaurant_read_stream_df: DataFrame, current_timestamp_utc) -> DataFrame:
    return (restaurant_read_stream_df
            .withColumn('trigger_datetime_created', current_timestamp_utc)
            .where("trigger_datetime_created between adv_campaign_datetime_start and adv_campaign_datetime_end")
            .withColumn('timestamp', f.from_unixtime(f.col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
            .dropDuplicates(['restaurant_id', 'timestamp'])
            .withWatermark('timestamp', '10 minutes'))

def read_subscribers_restaurant(spark: SparkSession) -> DataFrame:
    try:
        return (spark
                .read
                .format("jdbc")
                .option("url", config.POSTGRES_CLOUD_URL)
                .option("dbtable", config.POSTGRES_CLOUD_TABLE)
                .option("driver", "org.postgresql.Driver")
                .options(**postgresql_settings_cloud))
    except Exception as e:
        logger.error(f"Error read from PostgreSQL: {str(e)}")

def save_subscribers_feedback(result_df: DataFrame):
    try:
        return (result_df
                .withColumn("feedback", f.lit(None).cast(StringType()))
                .write
                .mode("append")
                .format("jdbc")
                .option("url", config.POSTGRES_LOCAL_URL)
                .option("dbtable", config.POSTGRES_LOCAL_TABLE)
                .option("driver", "org.postgresql.Driver")
                .options(**postgresql_settings_local)
                .save())
    except Exception as e:
        logger.error(f"Error write to PostgreSQL: {str(e)}")

def join(filtered_read_stream_df, subscribers_restaurant_df) -> DataFrame:
    """
    {
        "restaurant_id": идентификатор ресторана
        "adv_campaign_id": идентификатор рекламной акции
        "adv_campaign_content": текст рекламной акции
        "adv_campaign_owner": сотрудник ресторана, который является владельцем кампании
        "adv_campaign_owner_contact": контакт сотрудника ресторана
        "adv_campaign_datetime_start": время начала рекламной акции
        "adv_campaign_datetime_end": время окончания рекламной акции
        "datetime_created": время создания рекламной акции
        "client_id": идентификатор клиента
        "trigger_datetime_created": время создания выходного сообщения
    }
    """

    return (filtered_read_stream_df
    .join(subscribers_restaurant_df, on=["restaurant_id"], how="inner")
    .dropDuplicates(['client_id', 'adv_campaign_id', 'restaurant_id'])
    .withWatermark('timestamp', '1 minutes')
    .selectExpr([
        'restaurant_id',
        'adv_campaign_id',
        'adv_campaign_content',
        'adv_campaign_owner',
        'adv_campaign_owner_contact',
        'adv_campaign_datetime_start',
        'adv_campaign_datetime_end',
        "datetime_created",
        "client_id",
        'trigger_datetime_created'
    ]))

def foreach_batch_function(result_df, epoch_id):
    result_df.persist()
    save_subscribers_feedback(result_df)
    write_restaurant_stream(result_df)
    result_df.unpersist()

def run_query(result_df: DataFrame):
    return (result_df
            .writeStream
            .foreachBatch(foreach_batch_function)
            .start())


if __name__ == "__main__":
    spark = spark_init('restaurant_streaming_service')
    restaurant_read_stream_df = read_restaurant_stream(spark)
    filtered_read_stream_df = filtered_read_stream(restaurant_read_stream_df, f.unix_timestamp(f.current_timestamp()))
    subscribers_restaurant_df = read_subscribers_restaurant(spark)
    result_df = join(filtered_read_stream_df, subscribers_restaurant_df)

    query = run_query(result_df)

    while query.isActive:
        print(f"query information: runId={query.runId},"
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()