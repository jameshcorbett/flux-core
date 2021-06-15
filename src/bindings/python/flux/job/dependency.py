import uuid

from flux.job.JobID import JobID


class Dependency:

    def __init__(self, scheme, value, **kwargs):
        self.entry = {"scheme": scheme, "value": value, **kwargs}



    def __init__(self, uri):
        # replace first ':' with ':FXX' to work around urlparse refusal
        # to treat integer only path as a scheme:path.
        self.uri = urlparse(uri.replace(":", ":FXX", 1))

        if not self.uri.scheme or not self.uri.path:
            raise ValueError(f'Invalid dependency URI "{uri}"')

        self.path = self.uri.path.replace("FXX", "", 1)
        self.scheme = self.uri.scheme

    @staticmethod
    def _try_number(value):
        """Convert value to an int or a float if possible"""
        for _type in (int, float):
            try:
                return _type(value)
            except ValueError:
                continue
        return value

    @property
    def entry(self):
        uri = urlparse(uri.replace(":", ":FXX", 1))

        if not uri.scheme or not uri.path:
            raise ValueError(f'Invalid dependency URI "{uri}"')

        path = uri.path.replace("FXX", "", 1)
        scheme = uri.scheme
        if self.uri.query:
            for key, val in parse_qs(self.uri.query).items():
                #  val is always a list, but convert to single value
                #   if it only contains a single item:
                if len(val) > 1:
                    entry[key] = [self._try_number(x) for x in val]
                else:
                    entry[key] = self._try_number(val[0])
        return Dependency(scheme, path, )


def afterany_dependency(jobid):
    return {"scheme": "afterany", "value": JobID(jobid)}


def after_dependency(jobid):
    return {"scheme": "after", "value": JobID(jobid)}


def afterok_dependency(jobid):
    return {"scheme": "afterok", "value": JobID(jobid)}


def afternotok_dependency(jobid):
    return {"scheme": "afternotok", "value": JobID(jobid)}


def begintime_dependency(unix_seconds):
    return {"scheme": "begin-time", "value": float(unix_seconds)}
