import redis
import sys
import json

CHANNEL_NAMES = ["channel-1", "channel-2", "channel-3", "channel-4", "channel-5", "channel-6"]
r = redis.StrictRedis(host='localhost', port=6379, db=0)
p = r.pubsub()


class LoadBalancerClient(object):

    suffix = "value"

    def __init__(self, channel_ids, *args, **kwargs):
        """ Configure class and redis

        """
        self.channel_ids = channel_ids
        self.__dict__.update(kwargs)
        host = kwargs.get('host')
        port = kwargs.get('port')
        db = kwargs.get('db')
        self.redis_conn = redis.StrictRedis(host=host,port=port,db=db)


    def choose_other_channel(self, channel_id, channel_count, message):
        """  Pick other channel if it is subscribe """
        channel_count = dict((k, v) for k, v in channel_count.items() if k is not channel_id)
        try:
            min_ch = min(channel_count, key=channel_count.get)
            if self.redis_conn.publish(min_ch, message):
                self.redis_conn.incr("%s-%s" % (min_ch, self.suffix))
                return 'success'
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
                server = LoadBalancerServer(channel,
                                            host=self.__dict__['host'],
                                            port=self.__dict__['port'],
                                            db=self.__dict__['db'])
                channel_count[channel] = server.get_load_count()

        except redis.ConnectionError:
            raise Exception('Server Redis Connection is failed')

        min_ch = min(channel_count, key=channel_count.get)
        try:
            if self.redis_conn.publish(min_ch, message):
                self.redis_conn.incr("%(channel_name)s-%(suffix)s" % {'channel_name': min_ch, 'suffix':self.suffix})
                print 'success'
            else:
                self.choose_other_channel(min_ch,  channel_count, message)
        except redis.ConnectionError:
            raise Exception('Redis Connection is failed')

    # if __name__ == '__main__':
    #     a_game = Comm_system()

class LoadBalancerServer(object):

    suffix = "value"

    def __init__(self, channel_id, *args, **kwargs):
        self.channel_id = channel_id
        host = kwargs.get('host')
        port = kwargs.get('port')
        db = kwargs.get('db')
        self.redis_conn = redis.StrictRedis(host=host,port=port,db=db)

        #self.redis_conn = redis.StrictRedis(host='localhost',port=6379,db=0)

    def complete(self):
        """ Decrease the channel load by one """
        self.redis_conn.decr(self.channel_name(), 1)

    def channel_name(self):
        return ("%(channel_name)s-%(suffix)s" % {'channel_name': self.channel_id, 'suffix':self.suffix})

    # You can use getter and setter for the methods below
    def get_load_count(self):
        """ Return the load count stored for given channel """
        return int(self.redis_conn.get(self.channel_name()) or 0)

    def set_load_count(self):
        self.redis_conn.set(self.channel_name(), 0)

p.subscribe('channel-1')
client = LoadBalancerClient(CHANNEL_NAMES, host='localhost', port=6379,db=0)
b = client.distribute_message('kerem')
