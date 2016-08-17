import redis
import sys
import json

CHANNEL_NAMES = ["channel-1", "channel-2", "channel-3", "channel-4", "channel-5", "channel-6"]
r = redis.StrictRedis(host='localhost', port=6379, db=0)
p = r.pubsub()


class LoadBalancerClient(object):

    suffix = "value"

    def __init__(self, channel_ids, *args, **kwargs):
        """ Configure class and redis """
        self.channel_ids = channel_ids
        self.redis_conn = redis.StrictRedis(**kwargs)

    def choose_other_channel(self, channel_id, channel_count, message):
        """  Pick other channel if it is subscribe """
        channel_count = dict((k, v) for k, v in channel_count.items() if k is not channel_id)
        try:
            min_ch = min(channel_count, key=channel_count.get)
            if self.redis_conn.publish(min_ch, message):
                self.redis_conn.incr("%s-%s" % (min_ch, self.suffix))
                print 'success'
            else:
                self.choose_other_channel(min_ch,  channel_count, message)
        except ValueError:
            raise Exception('There is no subscribed channel to listen message')
        except redis.ConnectionError:
            raise Exception('Redis Connection is failed')

    def distribute_message(self, message):
        """ Pick a channel which is with the less load than other """
        try:
            channel_count = {}
            for channel in self.channel_ids:
                server = LoadBalancerServer(channel)
                channel_count[channel] = server.get_load_count()
        except redis.ConnectionError:
            raise Exception('Connection Error')
        min_ch = min(channel_count, key=channel_count.get)
        try:
            if self.redis_conn.publish(min_ch, message):
                self.redis_conn.incr("%s-%s" % (min_ch, self.suffix))
                print 'success'
            else:
                print '---'
                self.choose_other_channel(min_ch,  channel_count, message)
        except redis.ConnectionError:
            raise Exception('Redis Connection is failed')


class LoadBalancerServer(object):
    suffix = "value"

    def __init__(self, channel_id, *args, **kwargs):
        self.channel_id = channel_id
        self.redis_conn = redis.StrictRedis(**kwargs)

    def complete(self):
        """ Decrease the channel load by one """
        self.redis_conn.decr(self.channel_name, 1)

    @property
    def channel_name(self):
        return str('%s-%s' % (self.channel_id, self.suffix))

    # You can use getter and setter for the methods below
    def get_load_count(self):
        """ Return the load count stored for given channel """
        return int(self.redis_conn.get(self.channel_name) or 0)

    def set_load_count(self):
        self.redis_conn.set(self.channel_name, 0)
