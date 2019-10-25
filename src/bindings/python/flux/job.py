###############################################################
# Copyright 2014 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################
"""
This module provides three functions for submitting
and managing jobs: submit_async, submit, and submit_get_id.
See the help for those functions for more detail.
"""
import errno

import six

from flux.wrapper import Wrapper
from flux.util import check_future_error
from flux.future import Future
from _flux._core import ffi, lib


class JobWrapper(Wrapper):
    def __init__(self):
        super(JobWrapper, self).__init__(ffi, lib, prefixes=["flux_job_"])


RAW = JobWrapper()


def submit_async(flux_handle, jobspec, priority=lib.FLUX_JOB_PRIORITY_DEFAULT, flags=0):
    """Asynchronously submit a job to a Flux instance.

    The Flux instance to submit the job to, the job specification 
    and resource requirements, the job's priority, and the job 
    flags are taken as arguments. Return a Future object, which 
    can be waited on.
    
    For a blocking version of this call, see the submit() function, which
    takes the same argument and returns the job id of the submitted job.
    :param flux_handle: a handle to a Flux instance.
    :type flux_handle: a flux.Flux instance
    :param jobspec: a string specifying the job and its resource requirments
    :type jobspec: str or bytes
    :param priority: an integer priority for the job. Currently defaults to 16
    :type priority: int
    """
    if isinstance(jobspec, six.text_type):
        jobspec = jobspec.encode("utf-8")
    elif jobspec is None or jobspec == ffi.NULL:
        # catch this here rather than in C for a better error message
        raise EnvironmentError(errno.EINVAL, "jobspec must not be None/NULL")
    elif not isinstance(jobspec, six.binary_type):
        raise TypeError("jobpsec must be a string (either binary or unicode)")

    future_handle = RAW.submit(flux_handle, jobspec, priority, flags)
    return Future(future_handle)


@check_future_error
def submit_get_id(future):
    """Fetch the id of a submitted job. Block until the id is ready."""
    if future is None or future == ffi.NULL:
        raise EnvironmentError(errno.EINVAL, "future must not be None/NULL")
    future.wait_for()  # ensure the future is fulfilled
    jobid = ffi.new("flux_jobid_t[1]")
    RAW.submit_get_id(future, jobid)
    return int(jobid[0])


def submit(flux_handle, jobspec, priority=lib.FLUX_JOB_PRIORITY_DEFAULT, flags=0):
    """Submit a job to a Flux instance, block, and then return its jobid.

    The Flux instance to submit the job to, the job specification 
    and resource requirements, the job's priority, and the job 
    flags are taken as arguments. Returns the id of the submitted job

    For a nonblocking call, see submit_async(), which takes the same arguments
    but returns a Future object instead of a job id.
    :param flux_handle: a handle to a Flux instance.
    :type flux_handle: a flux.Flux instance
    :param jobspec: a string specifying the job and its resource requirments
    :type jobspec: str or bytes
    :param priority: an integer priority for the job. Currently defaults to 16
    :type priority: int
    """
    future = submit_async(flux_handle, jobspec, priority, flags)
    return submit_get_id(future)
