from .glob import ensure_global_client, flush_global_plan, get_inout_obj, pop_inout_obj
from .wrapper import (  # obj_to_debug_string,
    ResultPartProxy,
    ResultProxy,
    TaskOutput,
    _load,
)


def init():
    pass


def get_value_from_remote(obj):
    # if isinstance(obj, list):
    #    return [get_value_from_remote(o) for o in obj]
    # if isinstance(obj, tuple):
    #    return tuple([get_value_from_remote(o) for o in obj])

    # print("QE: GET_VALUE_FROM_REMOVE", obj_to_debug_string(obj))

    inout_obj = get_inout_obj(obj)
    if inout_obj is not None:
        # print("QE: REPLACED")
        obj = inout_obj

    if not isinstance(obj, (ResultProxy, ResultPartProxy, TaskOutput)):
        # print("QE: RETURNED", obj_to_debug_string(obj))
        return obj

    client = ensure_global_client()
    flush_global_plan(client)
    if isinstance(obj, TaskOutput):
        r = client.gather(obj.task, obj.output_id, 0)
    elif obj.task_outputs == 1:
        r = client.gather(obj.task, 0, obj.output_idx)
    else:
        r = client.gather(obj.task, obj.output_idx, 0)
    r = _load(r, False)
    # print("QE: GOT", obj_to_debug_string(r))
    return r


def delete_object(*objs):
    for obj in objs:
        _delete_object(obj)
    return "XXX"


def _delete_object(obj):
    inout_obj = pop_inout_obj(obj)
    if inout_obj is not None:
        obj = inout_obj

    if hasattr(obj, "task"):
        pass
        # client = ensure_global_client()
        # task = obj.task
        # TODO delete task from server
    #   print("QE: DELETE TASK", obj.task.state, obj.task)
    # else:
    #    print("QE: DELETE ", obj)
