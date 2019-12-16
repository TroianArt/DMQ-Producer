import requests


class Producer:
    def __init__(self, manager_url, queue_id, load_balancing_strategy="size"):
        self.__manager_host, self.__manager_port = manager_url
        self.__load_balancing_strategy = load_balancing_strategy
        self.__queue_id = queue_id
        self.__current_node = {}
        self.__current_request = 0
        self.number_of_requests = 10

    def get_statistics(self):
        response = requests.get(self.__manager_host + ':' + self.__manager_port + '/statistics/')
        return response.json()

    def balance_by_cpu(self):
        statistics = self.get_statistics()
        current_node = statistics['data_nodes'][0]
        for node in statistics['data_nodes']:
            if node["cpu_load"] <= current_node["cpu_load"]:
                current_node = node
        self.__current_node = current_node
        return self.__current_node

    def balance_by_size(self):
        statistics = self.get_statistics()
        current_node = statistics['data_nodes'][0]
        for node in statistics['data_nodes']:
            for queue in node['queues']:
                queue_id = queue['id']
                current_queue = list(filter(lambda x: x['id'] == queue_id, current_node['queues']))[0]
                if queue_id == self.__queue_id and queue['size'] <= current_queue['size']:
                    current_node = node
        self.__current_node = current_node
        return self.__current_node

    def rebalance(self):
        if self.__load_balancing_strategy == 'cpu':
            self.balance_by_cpu()
        self.balance_by_size()

    def produce(self, key, value):
        url = 'http://' + self.__manager_host + ':' + self.__manager_port
        response = requests.post(url, json={key: value})
        return response.json()


if __name__ == '__main__':
    producer = Producer(('http://127.0.0.1', '5000'), '0001', 'cpu')

    for i in range(15):
        message = {
            "text": "Message " + str(i)
        }

        producer.produce('messages', message)
