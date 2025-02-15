import redis


def get_redis_master(host, port, password=None, db=15):
    return redis.Redis(host=host, port=port, password=password, db=db, decode_responses=True)


if __name__ == '__main__':
    redis_master = get_redis_master('127.0.0.1', 6379)
