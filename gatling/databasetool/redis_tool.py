import json
from typing import Union

from databasetool.redis_base import get_redis_master
from utility.watch import watch_time


class RedisSet():
    def __init__(self, name, redis_master):
        self.name = name
        self.redis_master = redis_master

    @watch_time
    def add(self, *elems, show=False):
        res = self.redis_master.sadd(self.name, *elems)
        if show:
            print(f"{RedisSet.__class__.__name__} {self.name} add {len(elems)} elems")
        return res

    def pop(self):
        return self.redis_master.spop(self.name)

    def popn(self, n: int):
        return [sent for sent in self.redis_master.spop(self.name, n)]

    def remove(self, *elem):
        return self.redis_master.srem(self.name, *elem)

    def delete(self):
        return self.redis_master.delete(self.name)

    def __len__(self):
        return self.redis_master.scard(self.name)

    def __sub__(self, other: 'RedisSet'):
        elems = self.redis_master.sdiff([self.name, other.name])
        return {elem for elem in elems}

    def __and__(self, other: 'RedisSet'):
        elems = self.redis_master.sinter([self.name, other.name])
        return {elem for elem in elems}

    def __or__(self, other: 'RedisSet'):
        elems = self.redis_master.sunion([self.name, other.name])
        return {elem for elem in elems}

    def __contains__(self, elem: str):
        return self.redis_master.sismember(self.name, elem)

    def __iter__(self):
        elems = self.redis_master.smembers(self.name)
        for elem in elems:
            yield elem


class RedisDctn:

    def __init__(self, name, redis_master):
        self.name = name
        self.to_string = json.dumps
        self.fm_string = json.loads
        self.redis_master = redis_master

    def dict(self):
        return {k: self.fm_string(v) for k, v in self.redis_master.hgetall(self.name).items()}

    def keys(self):
        return [k for k in self.redis_master.hkeys(self.name)]

    def items(self):
        return self.dict().items()

    def values(self):
        return self.dict().values()

    def update(self, dctn: dict):
        dctn = {k: self.to_string(v) for k, v in dctn.items()}
        return self.redis_master.hset(self.name, mapping=dctn)

    def fetch(self, keys, default=None):
        values = [default if v is None else self.fm_string(v) for v in self.redis_master.hmget(self.name, keys)]
        return {k: v for k, v in zip(keys, values)}

    def remove(self, keys: str):
        return self.redis_master.hdel(self.name, keys)

    def delete(self):
        return self.redis_master.delete(self.name)

    def __len__(self):
        return self.redis_master.hlen(self.name)

    def __contains__(self, elem: object) -> bool:
        return self.redis_master.hexists(self.name, elem)

    def __setitem__(self, key: str, value: object):
        return self.redis_master.hset(self.name, key, self.to_string(value))

    def __getitem__(self, key: str):
        value = self.redis_master.hget(self.name, key)
        if value is None:
            raise KeyError(key)
        return self.fm_string(value)

    def get(self, key: str, default=None):
        value = self.redis_master.hget(self.name, key)
        if value is None:
            return default
        return self.fm_string(value)

    def __delitem__(self, key: str):
        return self.redis_master.hdel(self.name, key)

    def __iter__(self):
        return iter(self.keys())


class RedisQueue:

    def __init__(self, name, redis_master):
        self.name = name
        self.redis_master = redis_master

    def push(self, values: list[str]):
        if len(values) > 0:
            return self.redis_master.rpush(self.name, *values)
        return 0

    def pop(self, count=1):

        vals = self.redis_master.lpop(self.name, count=count)
        if vals is None:  return []

        return [val if val is None else val for val in vals]

    def __len__(self):
        return self.redis_master.llen(self.name)

    def __getitem__(self, key: Union[int, slice]):
        if isinstance(key, int):
            # return None if out of index
            res = self.redis_master.lindex(self.name, key)
            return res if res is None else res

        elif isinstance(key, slice):
            start = 0 if key.start is None else key.start
            stop = len(self) if key.stop is None else key.stop
            return [res if res is None else res for res in self.redis_master.lrange(self.name, start, stop)]
        else:
            raise KeyError(key)

    def __setitem__(self, key: int, value: str):
        return self.redis_master.lset(self.name, key, value)

    def __contains__(self, value: str):
        vs = self[:]
        return value in vs

    def remove(self, value: str):
        return self.redis_master.lrem(self.name, count=0, value=value)

    def delete(self):
        return self.redis_master.delete(self.name)


class RedisSortedSet:

    def __init__(self, name, redis_master):
        self.name = name
        self.redis_master = redis_master

    def __setitem__(self, key: str, value: Union[int, float]):
        return self.redis_master.zadd(self.name, {key: value})

    def __getitem__(self, key: str):
        if isinstance(key, str):
            value = self.redis_master.zscore(self.name, key)
            if value is None:
                raise KeyError(key)
            return (key, value)
        elif isinstance(key, slice):
            start = key.start if key.start is not None else 0
            stop = key.stop - 1 if key.stop is not None else -1

            if key.step is None:
                return self.redis_master.zrange(self.name, start, stop, withscores=True)  # 修正 stop
            elif key.step == -1:
                return self.redis_master.zrevrange(self.name, start, stop, withscores=True)
        else:
            raise TypeError("Key must be either a string or a slice.")

    def get(self, key: str, default=None):
        value = self.redis_master.zscore(self.name, key)
        if value is None:
            return default
        return value

    def score_range(self, min_score=None, max_score=None, start=0, num=None, reverse=False):
        min_score = min_score if min_score is not None else '-inf'
        max_score = max_score if max_score is not None else '+inf'
        if reverse:
            if num is None:
                return self.redis_master.zrevrangebyscore(self.name, max_score, min_score, withscores=True)
            else:
                return self.redis_master.zrevrangebyscore(self.name, max_score, min_score, start=start, num=num, withscores=True)
        else:
            if num is None:
                return self.redis_master.zrangebyscore(self.name, min_score, max_score, withscores=True)
            else:
                return self.redis_master.zrangebyscore(self.name, min_score, max_score, start=start, num=num, withscores=True)

    def __delitem__(self, key: str):
        return self.redis_master.zrem(self.name, key)

    def __len__(self):
        return self.redis_master.zcard(self.name)

    def list(self):
        return self.redis_master.zrange(self.name, 0, -1, withscores=True)

    def keys(self):
        return self.redis_master.zrange(self.name, 0, -1)

    def update(self, dctn: dict):
        # 确保分数是数值
        if not all(isinstance(value, (int, float)) for value in dctn.values()):
            raise ValueError("All scores must be integers or floats.")
        return self.redis_master.zadd(self.name, dctn)

    def remove(self, *keys):
        return self.redis_master.zrem(self.name, *keys)

    def delete(self):
        return self.redis_master.delete(self.name)

    def __contains__(self, elem: object) -> bool:
        result = self.redis_master.zscore(self.name, elem)

        return result is not None

    def __iter__(self):
        return iter(self.keys())


if __name__ == '__main__':
    pass

    REDIS_MASTER = get_redis_master(host='localhost', port=6379)
    if True:
        test_set = RedisSet('test_set', redis_master=REDIS_MASTER)
        test_set.add('a', 'b', 'c')
        assert len(test_set) == 3, "添加元素失败"

        # 测试移除元素
        test_set.remove('a')
        assert len(test_set) == 2, "移除元素失败"
        assert 'a' not in test_set, "元素'a'未被正确移除"

        # 测试并集
        other_set = RedisSet('otherset', redis_master=REDIS_MASTER)
        other_set.add('b', 'c', 'd')
        union_set = test_set | other_set
        assert union_set == {'b', 'c', 'd'}, "并集运算失败"

        # 测试交集
        inter_set = test_set & other_set
        assert inter_set == {'b', 'c'}, "交集运算失败"

        # 测试差集
        diff_set = test_set - other_set
        assert diff_set == set(), "差集运算失败"

        # 测试元素是否存在
        assert 'b' in test_set, "元素检测失败"
        assert 'a' not in test_set, "元素检测失败"

        # 测试迭代器
        for elem in test_set:
            assert elem in ['b', 'c'], "迭代失败"

        test_set.delete()
        other_set.delete()
        print(f"{RedisSet.__name__}所有测试用例通过！")

    if True:
        test_dctn = RedisDctn('testdctn', redis_master=REDIS_MASTER)

        # 测试添加元素
        test_dctn['a'] = 'apple'
        test_dctn['b'] = 'banana'
        test_dctn['c'] = 'cherry'
        assert len(test_dctn) == 3, "添加元素失败"

        # 测试获取元素
        assert test_dctn['a'] == 'apple', "获取元素失败"
        assert test_dctn.get('b') == 'banana', "获取元素失败"
        assert test_dctn.get('d', 'default') == 'default', "默认值返回失败"

        # 测试元素是否存在
        assert 'a' in test_dctn, "元素检测失败"
        assert 'd' not in test_dctn, "元素检测失败"

        # 测试删除元素
        del test_dctn['a']
        assert 'a' not in test_dctn, "删除元素失败"

        # 测试更新字典
        test_dctn.update({'c': 'coconut', 'd': 'date'})
        assert test_dctn['c'] == 'coconut', "更新元素失败"
        assert test_dctn['d'] == 'date', "添加新元素失败"

        # 测试 keys, values, items 方法
        assert set(test_dctn.keys()) == {'b', 'c', 'd'}, "Keys 方法失败"
        assert set(test_dctn.values()) == {'banana', 'coconut', 'date'}, "Values 方法失败"
        assert set(test_dctn.items()) == {('b', 'banana'), ('c', 'coconut'), ('d', 'date')}, "Items 方法失败"

        test_dctn.delete()

        print(f"{RedisDctn.__name__}所有测试用例通过！")

    if True:
        pass
        # 创建 RedisQueue 实例
        test_queue = RedisQueue('testqueue', redis_master=REDIS_MASTER)

        # 测试添加元素
        test_queue.push(['apple', 'banana', 'cherry'])
        assert len(test_queue) == 3, "添加元素失败"

        # 测试获取元素
        assert test_queue[0] == 'apple', "索引访问失败"
        assert test_queue[1] == 'banana', "索引访问失败"
        assert test_queue[-1] == 'cherry', "负索引访问失败"
        assert test_queue[1:3] == ['banana', 'cherry'], "切片访问失败"

        # 测试弹出元素
        popped_item = test_queue.pop()
        assert popped_item == ['apple'], "弹出元素失败"
        assert len(test_queue) == 2, "弹出元素后队列长度不正确"

        # 测试设置元素
        test_queue[1] = 'blueberry'
        assert test_queue[1] == 'blueberry', "设置元素失败"

        # 测试删除特定元素
        test_queue.remove('blueberry')
        assert 'blueberry' not in test_queue, "删除特定元素失败"

        # 测试清理队列
        test_queue.delete()
        assert len(test_queue) == 0, "清理队列失败"

        print(f"{RedisQueue.__name__} 所有测试用例通过！")
    if True:
        # 创建 RedisSortedSet 实例
        test_sorted_dctn = RedisSortedSet('test_sorted_dctn', redis_master=REDIS_MASTER)

        # 测试添加元素（包含小数、负数、正无穷）
        test_sorted_dctn['a'] = 1.5
        test_sorted_dctn['b'] = -2.5
        test_sorted_dctn['c'] = float('inf')
        test_sorted_dctn['d'] = 0
        test_sorted_dctn['e'] = 2
        assert len(test_sorted_dctn) == 5, "添加元素失败"

        # 测试获取单个元素
        assert test_sorted_dctn['a'] == ('a', 1.5), "获取元素失败"
        assert test_sorted_dctn['b'] == ('b', -2.5), "获取元素失败"
        assert test_sorted_dctn['c'] == ('c', float('inf')), "获取元素失败"
        assert test_sorted_dctn['d'] == ('d', 0), "获取元素失败"
        assert test_sorted_dctn['e'] == ('e', 2), "获取元素失败"

        # 测试获取切片（按分数排序）
        sliced_items = test_sorted_dctn[:3]
        assert sliced_items == [('b', -2.5), ('d', 0), ('a', 1.5)], "切片访问失败"

        # 测试获取不存在的元素
        try:
            test_sorted_dctn['z']
        except KeyError:
            pass
        else:
            assert False, "获取不存在的元素未抛出 KeyError"

        # 测试默认值获取
        assert test_sorted_dctn.get('z', 'default') == 'default', "默认值返回失败"

        # 测试范围查询（含负数和正无穷）
        range_items = test_sorted_dctn.score_range(min_score=-3, max_score=2)
        assert range_items == [('b', -2.5), ('d', 0), ('a', 1.5), ('e', 2)], "范围查询失败"

        reverse_range_items = test_sorted_dctn.score_range(min_score=-3, max_score=2, reverse=True)
        assert reverse_range_items == [('e', 2), ('a', 1.5), ('d', 0), ('b', -2.5)], "反向范围查询失败"

        # 测试更新（包含负无穷和普通分数）
        test_sorted_dctn.update({'f': -float('inf'), 'g': 3.5})
        assert len(test_sorted_dctn) == 7, "更新字典失败"
        assert test_sorted_dctn['f'] == ('f', -float('inf')), "更新后元素获取失败"
        assert test_sorted_dctn['g'] == ('g', 3.5), "更新后元素获取失败"

        # 测试删除元素
        del test_sorted_dctn['a']
        assert len(test_sorted_dctn) == 6, "删除元素失败"
        assert 'a' not in test_sorted_dctn, "删除元素失败"

        # 测试 keys 方法（按分数排序检查顺序）
        keys = list(test_sorted_dctn.keys())
        assert keys == ['f', 'b', 'd', 'e', 'g', 'c'], "Keys 顺序不正确"

        # 测试 list 方法
        all_items = test_sorted_dctn.list()
        assert all_items == [('f', -float('inf')), ('b', -2.5), ('d', 0), ('e', 2), ('g', 3.5), ('c', float('inf'))], "List 方法失败"

        # 测试元素是否存在
        assert 'b' in test_sorted_dctn, "元素检测失败"
        assert 'a' not in test_sorted_dctn, "元素检测失败"

        # 测试迭代器
        for idx, elem in enumerate(test_sorted_dctn):
            assert elem == keys[idx], "迭代顺序失败"

        # 测试删除多个元素
        test_sorted_dctn.remove(*['b', 'd'])
        assert len(test_sorted_dctn) == 4, "删除多个元素失败"
        assert 'b' not in test_sorted_dctn and 'd' not in test_sorted_dctn, "删除多个元素失败"
        
        # 测试清理
        test_sorted_dctn.delete()
        assert len(test_sorted_dctn) == 0, "清理失败"

        print(f"{RedisSortedSet.__name__} 所有测试用例通过！")
