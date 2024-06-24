from confluent_kafka import Producer
import json
import numpy as np

config = {
    'bootstrap.servers': 'localhost:9093',  # адрес Kafka сервера
    'client.id': 'simple-producer',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.username': 'admin',
    'sasl.password': 'admin-secret'
}

producer = Producer(**config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def send_message(data):
    try:
        # Асинхронная отправка сообщения
        producer.produce('test_topic', data.encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Поллинг для обработки обратных вызовов
    except BufferError:
        print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): try again")


def convert_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    raise TypeError


def main():
    from Connect_DB import connect_CH

    client = connect_CH()
    dataset = client.execute("""select toInt64(tare_id), 
                                    toInt64(office_id_load)
                                from tmp.skryl_pvz_wh 
                                limit 100""")

    for i in dataset:
        final_dataset = json.dumps(
            {
                'tare_id': convert_types(i[0]),
                'office_id_load': convert_types(i[1])
            }
        )

        send_message(final_dataset)
        producer.flush()


if __name__ == '__main__':
    main()
