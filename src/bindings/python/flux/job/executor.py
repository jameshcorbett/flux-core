###############################################################
# Copyright 2020 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################

import threading
import logging
import itertools
import collections
import concurrent.futures

import flux
import flux.job


class WaitingThread(threading.Thread):
    """Thread that waits for jobs to complete and fulfills futures."""

    def __init__(self, exit_event, future_queue, broker_args, broker_kwargs, **kwargs):
        super().__init__(**kwargs)
        self.__broker = flux.Flux(*broker_args, **broker_kwargs)
        self.__exit_event = exit_event
        self.__future_queue = future_queue
        self.__unrecognized_results = collections.deque()

    def run(self):
        """Loop indefinitely, marking futures as completed when jobs finish.

        Not all jobs returned from Flux may have an associated Future.
        Keep those jobs around, they may eventually get an associated Future.
        """
        jobid_mapping = {}  # map jobids to user-facing futures
        while not self.__exit_event.is_set():
            for result_fetcher in (self._get_new_result, self._get_cached_result):
                result = result_fetcher()
                if result is None:
                    continue  # no completed jobs, try again
                user_future = self._get_future_from_flux_jobid(
                    result.jobid, jobid_mapping
                )
                if user_future is None:
                    self.__unrecognized_results.append(result)
                else:
                    self._complete_future(user_future, result)

    def _get_future_from_flux_jobid(self, jobid_to_fetch, jobid_mapping):
        """Return the user-facing future associated with a jobid"""
        while self.__future_queue:  # collect any new user-facing futures
            jobid, future = self.__future_queue.popleft()
            jobid_mapping[jobid] = future
        return jobid_mapping.pop(jobid_to_fetch, None)

    def _get_new_result(self):
        """Return the latest completed job from Flux, or None."""
        try:
            return flux.job.wait(self.__broker)
        except OSError:  # no waitable jobs
            return None

    def _get_cached_result(self):
        """Return the first unrecognized job, or None."""
        try:
            return self.__unrecognized_results.popleft()
        except IndexError:  # no unrecognized results
            return None

    def _complete_future(self, user_future, result):
        """Mark a Future as completed with an error or a result."""
        if result.success:
            user_future.set_result(None)
        else:
            user_future.set_exception(
                Exception(f"Job exited abnormally: {result.errstr}")
            )


class SubmissionThread(threading.Thread):
    """Thread that, when started, submits jobspecs to Flux."""

    def __init__(
        self,
        exit_event,
        jobspecs_to_submit,
        jobid_future_pairs,
        broker_args,
        broker_kwargs,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.__exit_event = exit_event
        self.__jobspecs_to_submit = jobspecs_to_submit
        self.__jobid_future_pairs = jobid_future_pairs
        self.__broker = flux.Flux(*broker_args, **broker_kwargs)

    def run(self):
        """Loop indefinitely, submitting jobspecs and fetching jobids."""
        while not self.__exit_event.is_set():
            while self.__jobspecs_to_submit:
                jobspec, user_future = self.__jobspecs_to_submit.popleft()
                flux.job.submit_async(self.__broker, jobspec, waitable=True).then(
                    self._get_jobid_from_submission_future, user_future
                )
            if (
                self.__broker.reactor_run(
                    self.__broker.get_reactor(), flux.constants.FLUX_REACTOR_NOWAIT
                )
                < 0
            ):
                msg = "reactor start failed"
                self.__broker.fatal_error(msg)
                raise RuntimeError(msg)

    def _get_jobid_from_submission_future(self, submission_future, user_future):
        """Callback invoked when a jobid is ready for a submitted jobspec."""
        jobid = flux.job.submit_get_id(submission_future)
        user_future.set_jobid(jobid)
        self.__jobid_future_pairs.append((jobid, user_future))


class FluxExecutor:
    """Provides methods to submit jobs to Flux asynchronously.

    Heavily inspired by the ``concurrent.futures.Executor`` class.

    Forks two threads to complete futures in the background.

    :param thread_name_prefix: used to control the names of ``threading.Thread``
        objects created by the executor, for easier debugging.
    :param broker_args: positional arguments to the ``flux.Flux`` instances used by
        the executor.
    :param broker_args: keyword arguments to the ``flux.Flux`` instances used by
        the executor.
    """

    # Used to assign unique thread names when thread_name_prefix is not supplied.
    _counter = itertools.count().__next__

    def __init__(self, thread_name_prefix="", broker_args=(), broker_kwargs={}):
        self._submission_queue = collections.deque()
        self._jobid_future_pairs = collections.deque()
        self._shutdown_event = threading.Event()
        thread_name_prefix = (
            thread_name_prefix or f"{type(self).__name__}-{self._counter()}"
        )
        self._submission_thread = SubmissionThread(
            self._shutdown_event,
            self._submission_queue,
            self._jobid_future_pairs,
            broker_args,
            broker_kwargs,
            daemon=True,
            name=(thread_name_prefix + "-submission"),
        )
        self._submission_thread.start()
        self._waiting_thread = WaitingThread(
            self._shutdown_event,
            self._jobid_future_pairs,
            broker_args,
            broker_kwargs,
            daemon=True,
            name=(thread_name_prefix + "-waiting"),
        )
        self._waiting_thread.start()

    def shutdown(self):
        self._shutdown_event.set()

    def submit(self, jobspec):
        """Submit a jobspec to Flux and return a future."""
        fut = FluxExecutorFuture()
        fut.set_running_or_notify_cancel()
        self._submission_queue.append((jobspec, fut))
        return fut


class FluxExecutorFuture(concurrent.futures.Future):
    """A concurrent.futures.Future subclass that offers addititional jobid methods."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__jobid_condition = threading.Condition()
        self.__jobid = None
        self.__jobid_callbacks = []

    def set_jobid(self, jobid):
        """Sets the return value of work associated with the future.

        Should only be used by Executor implementations and unit tests.
        """
        if self.__jobid is not None:
            raise concurrent.futures.InvalidStateError()
        with self.__jobid_condition:
            self.__jobid = jobid
            self.__jobid_condition.notify_all()
        self.__invoke_jobid_callbacks()

    def jobid(self, timeout=None):
        """Return the jobid of the Flux job that the future represents.

        :param timeout: The number of seconds to wait for the jobid.
            If None, then there is no limit on the wait time.

        :returns: an integer jobid.

        :raises TimeoutError: If the jobid isn't available before the given
                timeout.
        """
        if self.__jobid is not None:
            return self.__jobid
        with self.__jobid_condition:
            self.__jobid_condition.wait(timeout)
            if self.__jobid is not None:
                return self.__jobid
            else:
                raise TimeoutError()

    def add_jobid_callback(self, fn):
        """Attaches a callable that will be called when the jobid is ready.


        :param fn: A callable that will be called with this future as its only
                argument when the future completes or is cancelled. The callable
                will always be called by a thread in the same process in which
                it was added. If the future has already completed or been
                cancelled then the callable will be called immediately. These
                callables are called in the order that they were added.
        """
        with self.__jobid_condition:
            if self.__jobid is None:
                self.__jobid_callbacks.append(fn)
                return
        try:
            fn(self)
        except Exception:
            logging.getLogger(__name__).exception(
                f"exception calling callback for {self}"
            )

    def __invoke_jobid_callbacks(self):
        for callback in self.__jobid_callbacks:
            try:
                callback(self)
            except Exception:
                logging.getLogger(__name__).exception(
                    f"exception calling callback for {self}"
                )
