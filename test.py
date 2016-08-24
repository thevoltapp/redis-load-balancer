import unittest
import redis
import sys
from io import TextIOWrapper, BytesIO

from loadbalancer import LoadBalancerClient, LoadBalancerServer

CHANNEL_NAMES = ["channel-1", "channel-2", "channel-3", "channel-4", "channel-5", "channel-6"]



class testLoadbalancer(unittest.TestCase):
    def setUp(self):
        self.r = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.p = self.r.pubsub(ignore_subscribe_messages=True)

    def test_client(self):
        client = LoadBalancerClient(CHANNEL_NAMES, host='localhost', port=6379, db=0)

        #test for if there are no subscribed channel
        with self.assertRaises(Exception) as context:
            client.distribute_message('volt')

        self.assertTrue('There is no subscribed channel to listen message' in context.exception)


        #test for distribute_message
        self.p.subscribe('channel-1')
        client.distribute_message('volt')


        while True:
            message = self.p.get_message()
            if message:
                message_result = message['data']
                message_channel = message['channel']
                break

        self.assertEqual(message_result, 'volt')
        self.assertEqual(message_channel, 'channel-1')


    def test_client_connection(self):
        #test for redis connection
        client = LoadBalancerClient(CHANNEL_NAMES, host='wrong_localhost', port=6379, db=0)

        # test for if there are no subscribed channel
        with self.assertRaises(Exception) as context:
            client.distribute_message('volt')


        self.assertTrue('Redis Connection is failed' in context.exception)

    def test_server(self):
        server = LoadBalancerServer('channel-test-value', host='localhost', port=6379, db=0)

        #test for get method
        count_from_module = server.get_load_count()
        count_from_redis = int(self.r.get('channel-test-value'))

        self.assertEqual(count_from_module,count_from_redis )

        #test for test method
        server.set_load_count()
        count_from_redis = int(self.r.get('channel-test-value'))
        self.assertEqual(0,count_from_redis)


if __name__ == '__main__':
    unittest.main()


