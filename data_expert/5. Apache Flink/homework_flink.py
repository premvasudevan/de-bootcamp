#   This flink job sessionizes the input data by IP address and host
#   ensure flink job container is up and running before executing this script
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.window import Session
from pyflink.table.expressions import col, lit


def create_events_table(t_env):
    kafka_key = os.environ["KAFKA_WEB_TRAFFIC_KEY"]
    kafka_secret = os.environ["KAFKA_WEB_TRAFFIC_SECRET"]
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    ddl = f"""
        CREATE TABLE {table_name} (
            url STRING,
            referrer STRING,
            user_id BIGINT,
            device_id BIGINT,
            host STRING,
            ip STRING,
            event_time_str STRING,
            event_time AS TO_TIMESTAMP(event_time_str, '{pattern}'),
            WATERMARK FOR event_time AS event_time - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{os.environ["KAFKA_TOPIC"]}',
            'properties.bootstrap.servers' = '{os.environ["KAFKA_URL"]}',
            'properties.group.id' = '{os.environ["KAFKA_GROUP"]}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """
    t_env.execute_sql(ddl)
    return table_name

def create_sessionized_events_sink_postgres(t_env):
    table_name = 'sessionized_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_time TIMESTAMP(3),
            event_count BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def log_session_stats():
    print('Starting the Job!')
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10000)
    print('checkpointing enabled')
    env.set_parallelism(2)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # Set up the table environment
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create a table from the Kafka source
    # This assumes the Kafka source is already set up in the environment
    try:
        source_table = create_events_table(t_env)
        postgres_sink = create_sessionized_events_sink_postgres(t_env)
        
        events = t_env.from_path(source_table)

        # Sessionize by ip and host
        events.window(
            Session.with_gap(lit(5).minutes).on(col("event_time")).alias("w")
        ).group_by(
            col("ip"), col("host"), col("w")
        ).select(
            col("ip"),
            col("host"),
            col("w").start.alias("session_time"),
            col("url").count.alias("event_count")
        ).execute_insert(postgres_sink)\
        .wait()


    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))  




if __name__ == '__main__':
    log_session_stats()