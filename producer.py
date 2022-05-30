from kafka import KafkaProducer
from datetime import datetime
import time
import pandas as pd

KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: x.encode('utf-8'))

    df = pd.read_csv('icu.csv')
    message_list = []
    message = None

    for i in range(df.shape[0]):
        i = i + 1
        message_fields_value_list = []
        print("Preparing message: " + str(i))
        event_datetime = datetime.now()

        for j in range(df.shape[1]):
            message_fields_value_list.append(df.iloc[i,j])

        message = ','.join(str(v) for v in message_fields_value_list)
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(0.5)

