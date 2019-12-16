from .__main__ import Producer

if __name__ == '__main__':
    producer = Producer(('http://127.0.0.1', '5000'), '0001', 'cpu')

    for i in range(15):
        message = {
            "text": "Message " + str(i)
        }

        producer.produce('messages', message)
