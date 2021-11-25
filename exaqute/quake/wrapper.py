import inspect
import logging
import os
import pickle

import cloudpickle
from quake.client.base.task import make_input, new_py_task
from quake.client.job import _set_rank
from quake.common.layout import Layout
from quake.job.config import JobConfiguration

from .glob import get_global_plan, get_inout_obj, set_inout_obj


def task_runner(jctx, input_data, python_job):
    _set_rank(jctx.rank)
    return python_job.run(input_data)


"""
def _load(obj):
    if isinstance(obj, bytes):
        return pickle.loads(obj)
    if len(obj) == 1:
        return _load(obj[0])
    return [_load(o) for o in obj]
"""


def collection_to_list(obj):
    if len(obj) > 16:
        return "..."
    else:
        return ",".join(obj_to_debug_string(o) for o in obj)


def obj_to_debug_string(obj):
    if isinstance(obj, list):
        return "List({})[{}]".format(len(obj), collection_to_list(obj))
    if isinstance(obj, tuple):
        return "Tuple({})[{}]".format(len(obj), collection_to_list(obj))
    # if isinstance(obj, dict) and len(obj) <= 20:
    #    return "Dict({})[{}]".format(len(obj), ",".join("{}:{}"
    #    .format(obj_to_debug_string(k), obj_to_debug_string(v)) in k, v in obj.items()))
    s = repr(obj)
    if len(s) > 20:
        return str(type(obj))
    else:
        return s


logger = logging.getLogger(__name__)


class PythonJob:
    def __init__(self, pickled_fn, task_args, task_returns, inouts, cwd):
        self.pickled_fn = pickled_fn
        self.task_args = task_args
        self.inouts = inouts
        self.cwd = cwd
        self.task_returns = task_returns

    def run(self, input_data):
        # kwargs = {name: pickle.loads(input_data[value]) for name, value in self.task_args.items()}
        kwargs = {}
        for name, value in self.task_args.items():
            logger.info("Arg %s => %s", name, obj_to_debug_string(value))
            value = replace_placeholders(value, input_data)
            kwargs[name] = value
            logger.info("Set arg %s => %s", name, obj_to_debug_string(value))

        result = cloudpickle.loads(self.pickled_fn)(**kwargs)
        # print("RETURNING", obj_to_debug_string(result))
        # print("RETURNS", self.task_returns)
        # print("INOUTS", self.inouts)
        if self.task_returns == 1:
            logger.info("Pickling individual output")
            output = [pickle.dumps(result)]
        else:
            if self.task_returns != len(result):
                raise Exception(
                    "Expecting {} outputs, but got {}".format(
                        self.task_returns, len(result)
                    )
                )
            logger.info("Pickling %s outputs", len(result))
            output = [pickle.dumps(r) for r in result]
        inouts = [pickle.dumps(kwargs[name]) for name in self.inouts]
        # print("RETURNING INOUTS", obj_to_debug_string(inouts))
        return output + inouts


# ArgConfig = collections.namedtuple("ArgConfig", "layout")


class ArgConfig:
    def __init__(self, inout):
        self.inout = inout
        self.layout = "all_to_all"
        self.unwrap = True


call_id_counter = 0


class WrapperConfig:
    def __init__(self, fn, default_keep, n_processes, n_outputs, arg_configs):
        self.fn = fn
        self.signature = inspect.signature(fn)
        self.default_keep = default_keep
        self.n_processes = n_processes
        self.n_outputs = n_outputs
        self._pickled_fn = None
        self.inouts = []
        self.arg_configs = {}

        for name, value in arg_configs.items():
            if isinstance(value, str):
                tp = value
            elif isinstance(value, dict):
                tp = value.get("type")
            else:
                raise Exception("Invalid value: '{}'".format(value))
            if tp in (None, "collection_in", "in", "fileout"):
                inout = None
            elif tp == "inout":
                inout = len(self.inouts)
                self.inouts.append(name)
            else:
                raise Exception("Invalid type {}".format(tp))
            self.arg_configs[name] = ArgConfig(inout)

    def _update_mpi_args(self, n_processes, args):
        self.n_processes = n_processes
        for name, value in args.items():
            assert name.endswith("_layout")
            name = name[: -len("_layout")]
            # print("QE: MPI_ARG", name)
            block_length = value["block_length"]
            block_count = value["block_count"]
            stride = value["stride"]

            assert stride == 1
            assert block_length == 1
            assert block_count == n_processes

            config = self.arg_configs.get(name)
            if config is None:
                config = ArgConfig(False)
                self.arg_configs[name] = config
            config.layout = "scatter"
            config.unwrap = True

    def pickle_fn(self):
        if self._pickled_fn:
            return self._pickled_fn
        else:
            self._pickled_fn = cloudpickle.dumps(self.fn)
            return self._pickled_fn

    def _prepare_inputs(self, args, kwargs):
        # if "self" in self.signature.parameters:
        #    binding = self.signature.bind(None, *args, **kwargs)
        # else:
        #    binding = self.signature.bind(*args, **kwargs)
        binding = self.signature.bind(*args, **kwargs)
        inputs = []
        args = {}
        for name, value in binding.arguments.items():
            arg_config = self.arg_configs.get(name)
            if arg_config:
                layout = arg_config.layout
                unwrap = arg_config.unwrap
            else:
                layout = "all_to_all"
                unwrap = True
            args[name] = put_placeholders(value, inputs, 0, layout, unwrap)
            """
            if inout_obj is not None:
                value = inout_obj
            if isinstance(value, ResultProxy):
                if arg_config:
                    layout = arg_config.layout
                else:
                    layout = "all_to_all"
                task_args[name] = len(inputs)
                inputs.append(make_input(value.task, list(range(value.n_outputs)), layout=layout))
            elif isinstance(value, ResultPartProxy):
                if arg_config:
                    layout = arg_config.layout
                else:
                    layout = "all_to_all"
                task_args[name] = len(inputs)
                inputs.append(make_input(value.task, [value.output_id], layout=layout))
            else:
                assert not isinstance(value, Task)
                if arg_config and arg_config.layout != "all_to_all":
                    raise Exception("Non-task result is used as argument with layout")
                const_args[name] = value
            """
        return inputs, args, binding

    def __repr__(self):
        return "<FunctionWrapper of '{}'>".format(self.fn.__class__.__name__)

    def make_task(self, *args, keep=None, returns=None, **kwargs):
        global call_id_counter
        call_id_counter += 1
        call_id = call_id_counter
        # print("QE: [{}] CALLING {}".format(call_id, self.fn))
        # traceback.print_stack()
        # if self.fn.__name__ == "updatePartialQoiEstimators_Task":
        #    print(args)
        #    xxx()
        if keep is None:
            keep = self.default_keep
        if returns is None:
            returns = self.n_outputs
        else:
            assert isinstance(returns, int)

        inputs, task_args, binding = self._prepare_inputs(args, kwargs)

        if self.n_processes != 1:
            assert returns == self.n_processes
            task_returns = 1
        else:
            task_returns = returns

        cwd = os.getcwd()
        real_returns = task_returns + len(self.inouts)
        payload = PythonJob(self.pickle_fn(), task_args, task_returns, self.inouts, cwd)
        config = pickle.dumps(JobConfiguration(task_runner, real_returns, payload))
        task = new_py_task(
            real_returns + len(self.inouts), self.n_processes, keep, config, inputs
        )
        task.config["env"] = dict(os.environ)
        task.call_id = call_id
        get_global_plan().add_task(task)

        for i, name in enumerate(self.inouts):
            obj = binding.arguments[name]
            set_inout_obj(obj, TaskOutput(task, returns + i))

        return ResultProxy(task, task_returns)


def put_placeholders(obj, inputs, level=0, layout="all_to_all", unwrap=False):
    inout_obj = get_inout_obj(obj)
    if inout_obj is not None:
        assert level == 0
        # print("%%% REPLACING", obj_to_debug_string(obj), " ---> ", inout_obj)
        obj = inout_obj
    if isinstance(obj, ResultProxy):
        placeholder = Placeholder(len(inputs), unwrap)
        inp = make_input(obj.task, list(range(obj.task_outputs)), layout=layout)
        inputs.append(inp)
        return placeholder
    if isinstance(obj, ResultPartProxy):
        assert layout == "all_to_all"
        placeholder = Placeholder(len(inputs), unwrap)
        inp = make_input(
            obj.task,
            list(range(obj.task_outputs)),
            layout=Layout(0, obj.output_idx, 0, 1),
        )
        inputs.append(inp)
        return placeholder
    if isinstance(obj, TaskOutput):
        placeholder = Placeholder(len(inputs), unwrap)
        inp = make_input(obj.task, obj.output_id)
        inputs.append(inp)
        return placeholder
    elif isinstance(obj, list):
        level += 1
        return [put_placeholders(o, inputs, level, "all_to_all", unwrap) for o in obj]
    else:
        return obj


def _load(obj, unwrap):
    if isinstance(obj, bytes):
        return pickle.loads(obj)
    if unwrap and len(obj) == 1:
        return _load(obj[0], False)
    return [_load(o, False) for o in obj]


def replace_placeholders(obj, inputs):
    if isinstance(obj, Placeholder):
        data = inputs[obj.index]
        return _load(data, obj.unwrap)
    elif isinstance(obj, list):
        return [replace_placeholders(o, inputs) for o in obj]
    else:
        return obj


class Placeholder:
    def __init__(self, index, unwrap):
        self.index = index
        self.unwrap = unwrap


class ResultProxy:

    __slots__ = ["task", "task_outputs", "n_outputs"]

    def __init__(self, task, task_outputs):
        self.task = task
        self.task_outputs = task_outputs
        self.n_outputs = task_outputs * task.n_workers

    def __len__(self):
        return self.n_outputs

    def __getitem__(self, idx):
        if not 0 <= idx < self.n_outputs:
            raise Exception(
                "Asked element at index {}, but there are only {} parts".format(
                    idx, self.n_outputs
                )
            )
        return ResultPartProxy(self.task, self.task_outputs, idx)

    def __iter__(self):
        return iter(
            ResultPartProxy(self.task, self.task_outputs, output_id)
            for output_id in range(self.n_outputs)
        )


class ResultPartProxy:

    __slots__ = ["task", "task_outputs", "output_idx"]

    def __init__(self, task, task_outputs, output_idx):
        self.task = task
        self.task_outputs = task_outputs
        self.output_idx = output_idx

    def __repr__(self):
        return "<RPP task={} outputs={} idx={}>".format(
            self.task, self.task_outputs, self.output_idx
        )


class TaskOutput:

    __slots__ = ["task", "output_id"]

    def __init__(self, task, output_id):
        self.task = task
        self.output_id = output_id

    def __repr__(self):
        return "<TaskOutput task_id={} output_id={}>".format(
            self.task.task_id, self.output_id
        )
