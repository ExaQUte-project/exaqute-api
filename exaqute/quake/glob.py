import os

import cloudpickle
from quake.client.base.client import Client
from quake.client.base.plan import Plan

_global_plan = Plan()
_global_client = None
_pickle_cache = {}
_inouts_objs = {}


def get_inout_obj(obj):
    pair = _inouts_objs.get(id(obj))
    if pair:
        return pair[0]
    else:
        return None


def set_inout_obj(obj, value):
    _inouts_objs[id(obj)] = (value, obj)


def pop_inout_obj(obj):
    obj_id = id(obj)
    if obj_id is not _inouts_objs:
        return None
    pair = _inouts_objs.pop(obj_id)
    return pair[0]


def pickle_function(fn):
    data = _pickle_cache.get(fn)
    if data is None:
        data = cloudpickle.dumps(fn)
        _pickle_cache[fn] = data
    return data


def get_global_plan():
    return _global_plan


def flush_global_plan(client):
    tasks = _global_plan.take_tasks()
    if tasks:
        client.submit(tasks)
        # for task in tasks:
        #    print("QE: task_id = {} <-> call_id = {}, outputs={}"
        #    .format(task.task_id, task.call_id, task.n_outputs))


def ensure_global_client():
    global _global_client
    if _global_client is None:
        server = os.environ.get("QUAKE_SERVER")
        if server is None:
            raise Exception(
                "No global server is defined."
                "Set variable QUAKE_SERVER or call quake.client.set_global_client()"
            )
        if ":" in server:
            hostname, port = server.rsplit(":", 1)
            try:
                port = int(port)
            except ValueError:
                raise Exception("Invalid format of QUAKE_SERVER variable")
            _global_client = Client(hostname, port)
        else:
            _global_client = Client(server)
    return _global_client
