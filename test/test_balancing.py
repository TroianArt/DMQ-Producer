import requests
from producer.__main__ import *
import unittest
from unittest import mock


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'http://127.0.0.1:9000/statistics/':
        statistics = {
            "data_nodes": [
                {
                    "address": "127.0.0.1",
                    "port": "5000",
                    "cpu_load": 0.34,
                    "queues": [
                        {
                            "id": "0001",
                            "name": "queue1",
                            "size": 3
                        },
                        {
                            "id": "0002",
                            "name": "queue2",
                            "size": 4
                        }
                    ]
                },
                {
                    "address": "127.0.0.1",
                    "port": "5001",
                    "cpu_load": 0.66,
                    "queues": [
                        {
                            "id": "0001",
                            "name": "queue1",
                            "size": 5
                        },
                        {
                            "id": "0002",
                            "name": "queue2",
                            "size": 6
                        }
                    ]
                }
            ]
        }
        return MockResponse(statistics, 200)

    return MockResponse(None, 404)


class TestProducer(unittest.TestCase):
    def setUp(self):
        self.example_producer = Producer(('http://127.0.0.1', '9000'), '0001')
        self.data_node = {
            "address": "127.0.0.1",
            "port": "5000",
            "cpu_load": 0.34,
            "queues": [
                {
                    "id": "0001",
                    "name": "queue1",
                    "size": 3
                },
                {
                    "id": "0002",
                    "name": "queue2",
                    "size": 4
                }
            ]
        }

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_fetch(self, get):
        balanced_data_node_by_cpu = self.example_producer.balance_by_cpu()
        self.assertDictEqual(self.data_node, balanced_data_node_by_cpu)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_fetch2(self, get):
        balanced_data_node_bu_size = self.example_producer.balance_by_size()
        self.assertDictEqual(self.data_node, balanced_data_node_bu_size)


if __name__ == 'main':
    unittest.main()
