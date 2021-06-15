import uuid

from flux.job.JobID import JobID


TYPE_IN = "in"
TYPE_OUT = "out"
TYPE_INOUT = "inout"
_TYPES = {TYPE_INOUT, TYPE_IN, TYPE_INOUT}

SCOPE_USER = "user"
SCOPE_GLOBAL = "global"
_SCOPES = {SCOPE_GLOBAL, SCOPE_USER}


class Dependency:

    def __init__(self, scheme, value, **kwargs):
        self.entry = {"scheme": scheme, "value": value, **kwargs}



def afterany_dependency(jobid):
    return Dependency("afterany", JobID(jobid))


def after_dependency(jobid):
    return Dependency("after", JobID(jobid))


def afterok_dependency(jobid):
    return Dependency("afterok", JobID(jobid))


def afternotok_dependency(jobid):
    return Dependency("afternotok", JobID(jobid))


def begintime_dependency(unix_seconds):
    return Dependency("begin-time", float(unix_seconds))


def string_dependency(value, dep_type, scope=SCOPE_USER):
    _validate_scope_type(dep_type, scope)
    return Dependency("string", value, type=dep_type, scope=scope)


def fluid_dependency(value, dep_type, scope=SCOPE_USER):
    _validate_scope_type(dep_type, scope)
    return Dependency("fluid", value, type=dep_type, scope=scope)


def _validate_scope_type(dep_type, scope):
    if dep_type not in _TYPES:
        raise ValueError(f"Unrecognized dependency type {dep_type}")
    if scope not in _SCOPES:
        raise ValueError(f"Unrecognized dependency scope {scope}")


def fan_out(from_spec, to_specs, name=None):
    name = _get_name(name)
    from_spec.add_dependency(string_dependency(name, TYPE_OUT))
    for jobspec in to_specs:
        jobspec.add_dependency(string_dependency(name, TYPE_IN))


def fan_in(from_specs, to_spec, name=None):
    name = _get_name(name)
    to_spec.add_dependency(string_dependency(name, TYPE_IN))
    for jobspec in from_specs:
        jobspec.add_dependency(string_dependency(name, TYPE_OUT))


def chain(jobspecs, name=None):
    name = _get_name(name)
    for jobspec in jobspecs:
        jobspec.add_dependency(string_dependency(name, TYPE_INOUT))


def _get_name(name):
    if name is None:
        name = uuid.uuid4()
    return str(name)
