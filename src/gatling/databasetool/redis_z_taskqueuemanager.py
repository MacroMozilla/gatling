import json
import datetime
import traceback
import time
from typing import List

import numpy as np
import pandas as pd
from tqdm import trange

from gatling.databasetool.redis_tool import RedisQueue, RedisDctn
from gatling.utility.batch_tools import K_kwargs
from gatling.utility.const import K_uid, K_args, K_result
from gatling.utility.decorator_tools import show_process_and_thread_id
from gatling.utility.idx_tools import get_unique_task_id
from gatling.utility.watch import Watch, watch_time


class ResultList(list):
    """
    一个继承自 list 的自定义类。

    初始化时需要传入：
    - results: 用于初始化列表的数据（应为可迭代对象）。
    - redisdctn: 存储额外信息的参数，可以是任意类型。
    """

    def __init__(self, results: List, redisdctn: RedisDctn):
        # 使用 results 初始化父类 list
        super().__init__(results)
        self.redisdctn = redisdctn

    def delete(self):
        N = len(self.redisdctn)
        self.redisdctn.delete()
        print(f"delete {self.redisdctn.__class__.__name__} {self.redisdctn.name} , #results = {N}")


class RedisTaskQueueManager:

    def __init__(self, fctn, redis_master):
        self.fctn = fctn
        self.redis_master = redis_master
        self.fctn_key_waiting = f'temp:TaskQueue:{fctn.__name__}:waiting'
        self.fctn_key_executing = f'temp:TaskDctn:{fctn.__name__}:executing'
        self.fctn_key_error = f'temp:TaskDctn:{fctn.__name__}:error'
        self.fctn_key_result = f'temp:TaskDctn:{fctn.__name__}:rpack'

        self.redisqueue_waiting = RedisQueue(name=self.fctn_key_waiting, redis_master=self.redis_master)
        self.redisdctn_executing = RedisDctn(name=self.fctn_key_executing, redis_master=self.redis_master)
        self.redisdctn_error = RedisDctn(name=self.fctn_key_error, redis_master=self.redis_master)
        self.redisdctn_rpack = RedisDctn(name=self.fctn_key_result, redis_master=self.redis_master)

    def reset_params(self):
        self.redisqueue_waiting.delete()
        self.redisdctn_executing.delete()
        self.redisdctn_error.delete()
        print(f"reset {self.redisqueue_waiting.__class__.__name__} {self.redisqueue_waiting.name}")
        print(f"reset {self.redisdctn_executing.__class__.__name__} {self.redisdctn_executing.name}")
        print(f"reset {self.redisdctn_error.__class__.__name__} {self.redisdctn_error.name}")

    def reset_rpacks(self):
        self.redisdctn_rpack.delete()
        print(f"reset {self.redisdctn_rpack.__class__.__name__} {self.redisdctn_rpack.name}")

    def push_waiting(self, argskwargs_s):
        args_kwargs_uid_sent_s = []
        for argskwargs in argskwargs_s:
            argskwargs[K_uid] = get_unique_task_id()
            args_kwargs_uid_sent_s.append(json.dumps(argskwargs))
        self.redisqueue_waiting.push(args_kwargs_uid_sent_s)
        print(f"push {len(argskwargs_s)} items to {self.redisqueue_waiting.__class__.__name__} {self.redisqueue_waiting.name}")

    def get_len_waiting(self):
        return len(self.redisqueue_waiting)

    def batch_execute(self, ongoing=False):

        while True:
            pre_N = len(self.redisqueue_waiting)

            if pre_N > 0:
                w = Watch()
                while pre_N > 0:
                    # pop from waiting to executing
                    args_kwargs_uid_sent_s = self.redisqueue_waiting.pop()

                    for args_kwargs_uid_sent in args_kwargs_uid_sent_s:
                        self.redisdctn_executing[args_kwargs_uid_sent] = f'{datetime.datetime.now()}'
                        args_kwargs_uid = json.loads(args_kwargs_uid_sent)
                        try:
                            args = args_kwargs_uid.get(K_args, [])
                            kwargs = args_kwargs_uid.get(K_kwargs, {})
                            uid = args_kwargs_uid[K_uid]
                            result = show_process_and_thread_id(self.fctn)(*args, **kwargs)

                            rpack = {}
                            if args:
                                rpack[K_args] = args
                            if kwargs:
                                rpack[K_kwargs] = kwargs
                            rpack[K_result] = result

                            self.redisdctn_rpack[uid] = json.dumps(rpack)

                            print(f"SUCCESS {self.fctn.__name__} {args_kwargs_uid}", end='\t')
                        except Exception as e:
                            err_msg = traceback.format_exc()
                            print(err_msg)
                            self.redisdctn_error[args_kwargs_uid_sent] = err_msg
                            print(f"ERROR {self.fctn.__name__} {args_kwargs_uid}", end='\t')

                        finally:

                            cur_N = len(self.redisqueue_waiting)
                            cost_single = w.see_timedelta()

                            finished_N = pre_N - cur_N
                            cost_multi = cost_single / finished_N if finished_N > 0 else pd.Timedelta('NaT')
                            estimate = cost_multi * cur_N

                            print(f'cost {round(cost_single.total_seconds(), 4)}/{finished_N} = {round(cost_multi.total_seconds(), 4)} remain {cur_N} estimate {estimate}')

                            pre_N = cur_N

                            del self.redisdctn_executing[args_kwargs_uid_sent]

                print('TASK DONE !')
                print(f"total cost {w.total_timedelta()}")
            else:
                if not ongoing:
                    break
                else:
                    pass
                    seconds = 60
                    print(f'SLEEP AT {datetime.datetime.now()}')
                    print(f"SLEEP {seconds} secs ......")

                    for _ in trange(seconds, desc="Sleeping", unit="sec"):
                        time.sleep(1)

    def check_done_block(self, check_interval=5, get_pids_fctn=None):
        """
            使用单次调用 check_done 获取结果并处理
            """

        watch_for_cost = Watch()
        prev_remaining = len(self.redisqueue_waiting)

        while prev_remaining > 0:
            cur_remaining = len(self.redisqueue_waiting)

            processed_items_cost = watch_for_cost.see_timedelta()
            processed_items_num = prev_remaining - cur_remaining

            per_item_cost = processed_items_cost / processed_items_num if processed_items_num > 0 else np.inf

            remain_item_cost_est = per_item_cost * cur_remaining

            per_item_cost_seconds = round(per_item_cost.total_seconds(), 3) if per_item_cost != np.inf else np.inf
            processed_items_cost = round(processed_items_cost.total_seconds(), 3)

            pids_sent = "get_pids_fctn is undefined."
            if get_pids_fctn is not None:
                pids_sent = f"pids = {get_pids_fctn()}"

            rate_sent = f"{processed_items_cost} sec / {processed_items_num} item = {per_item_cost_seconds} sec/item"
            if per_item_cost_seconds <1.0:
                rate_sent = f"{processed_items_num} item / {processed_items_cost} sec / = {round(1/per_item_cost_seconds,3)} item/sec"


            print(f"""{self.fctn.__name__} cost {rate_sent}, remain {cur_remaining} items, estimate {remain_item_cost_est}, {pids_sent}""")

            prev_remaining = cur_remaining

            time.sleep(check_interval)

        while len(self.redisdctn_executing) > 0:
            time.sleep(0.01)

        print(f'{self.fctn.__name__} done!')

    def fetch_results(self):
        uid2rpack = self.redisdctn_rpack.dict()
        uids = sorted(list(uid2rpack.keys()))
        results = [json.loads(uid2rpack[uid]).get(K_result, None) for uid in uids]
        rlist = ResultList(results, self.redisdctn_rpack)

        return rlist

    def fetch_rpacks_args_kwargs(self):
        uid2rpack = self.redisdctn_rpack.dict()
        uids = sorted(list(uid2rpack.keys()))
        rpacks = [json.loads(uid2rpack[uid]) for uid in uids]

        argskwargs_s = []
        for rpack in rpacks:
            args = rpack.get(K_args, [])
            kwargs = rpack.get(K_kwargs, {})

            argskwargs = {}
            if len(args) > 0:
                argskwargs[K_args] = args
            if len(kwargs) > 0:
                argskwargs[K_kwargs] = kwargs
            argskwargs_s.append(argskwargs)

        return argskwargs_s

    def restore_executing(self):
        argskwargs_sent_s = (self.redisdctn_executing.keys())
        self.redisqueue_waiting.push(argskwargs_sent_s)
        self.redisdctn_executing.delete()

    def restore_error(self):
        argskwargs_sent_s = (self.redisdctn_error.keys())
        self.redisqueue_waiting.push(argskwargs_sent_s)
        self.redisdctn_error.delete()


if __name__ == '__main__':
    pass
