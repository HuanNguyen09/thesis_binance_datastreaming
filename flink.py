import redis
import logging
import sys
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
import traceback 


def write_to_redis(record):
    try:
        # Connect to Redis
        redis_host = "localhost"
        redis_port = 6379
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

        # Convert Row to dictionary
        record_dict = {
            "f0": record[0],
            "f1": record[1],
            "f2": record[2],
            "f3": record[3],
            "f4": record[4],
            "f5": record[5],
            "f6": record[6],
            "f7": record[7],
            "f8": record[8],
            "f9": str(record[9]),  # Convert boolean to string
            "f10": str(record[10])  # Convert boolean to string
        }
        
        print(record_dict)
        # Write data to Redis using XADD
        redis_client.xadd("my-stream", fields=record_dict)

    except Exception as e:
        # Log the error
        print("Error writing to Redis:", e)
        traceback.print_exc()  # Print the stack trace


def write_to_kafka(env):
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    ds = env.from_collection(
        [(1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=type_info)

    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    kafka_producer = FlinkKafkaProducer(
        topic='test_json_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute()


def read_from_kafka(env):
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
    .type_info(Types.ROW([
        Types.STRING(),  # e
        Types.LONG(),    # E
        Types.STRING(),  # s
        Types.LONG(),    # t
        Types.STRING(),  # p
        Types.STRING(),  # q
        Types.LONG(),    # b
        Types.LONG(),    # a
        Types.LONG(),    # T
        Types.BOOLEAN(), # m
        Types.BOOLEAN()  # M
    ])) \
    .build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='test2',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092'}
    )
    kafka_consumer.set_start_from_earliest()
    # Write data to Redis
    # Read data from Kafka
    kafka_stream = env.add_source(kafka_consumer)
    kafka_stream.map(write_to_redis)
    env.add_source(kafka_consumer).print()
    env.execute()
    

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:/home/huannguyen/Data/KLTN/flink-sql-connector-kafka-1.15.0.jar")
    env.set_python_executable("/usr/bin/python3")
    # print("start writing data to kafka")
    # write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)

    