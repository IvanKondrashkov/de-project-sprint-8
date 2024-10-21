import os

TOPIC_NAME_REQ = os.getenv('TOPIC_NAME_REQ', 'student.topic.cohort27.shuffleto.req')
TOPIC_NAME_RESP = os.getenv('TOPIC_NAME_RESP', 'student.topic.cohort27.shuffleto.resp')
KAFKA_URL = os.getenv('KAFKA_URL', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
KAFKA_USER = os.getenv('KAFKA_USER', 'de-student')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'ltcneltyn')

POSTGRES_CLOUD_URL = os.getenv('POSTGRES_CLOUD_URL', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de')
POSTGRES_CLOUD_USER = os.getenv('POSTGRES_CLOUD_USER', 'student')
POSTGRES_CLOUD_PASSWORD = os.getenv('POSTGRES_CLOUD_PASSWORD', 'de-student')
POSTGRES_CLOUD_TABLE = os.getenv('POSTGRES_CLOUD_TABLE', 'subscribers_restaurants')

POSTGRES_LOCAL_URL = os.getenv('POSTGRES_LOCAL_URL', 'jdbc:postgresql://localhost:5432/de')
POSTGRES_LOCAL_USER = os.getenv('POSTGRES_LOCAL_USER', 'jovyan')
POSTGRES_LOCAL_PASSWORD = os.getenv('POSTGRES_LOCAL_PASSWORD', 'jovyan')
POSTGRES_LOCAL_TABLE = os.getenv('POSTGRES_LOCAL_TABLE', 'subscribers_feedback')