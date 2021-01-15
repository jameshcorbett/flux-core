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
import atexit

import flux
import flux.job


class JobFailure(Exception):
    pass


class FluxExecutorThread(threading.Thread):
    """Thread that, when started, submits jobspecs to Flux."""

    def __init__(
        self,
        exit_event,
        jobspecs_to_submit,
        event_timer,
        broker_args,
        broker_kwargs,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.__exit_event = exit_event
        self.__jobspecs_to_submit = jobspecs_to_submit
        self.__outstanding_futures = 0
        self.__event_timer = event_timer
        self.__broker = flux.Flux(*broker_args, **broker_kwargs)

    def run(self):
        """Loop indefinitely, submitting jobspecs and fetching jobids."""
        self.__broker.timer_watcher_create(self.__event_timer, self.__wait_on_new_jobs, repeat=self.__event_timer).start()
        while self.__work_remains():
            if self.__broker.reactor_run() < 0:
                msg = "reactor start failed"
                self.__broker.fatal_error(msg)
                raise RuntimeError(msg)

    def __work_remains(self):
        """Return True if and only if there is still work to be done.

        Equivalently, return False if it is safe to exit.
        """
        return (not self.__exit_event.is_set() or self.__jobspecs_to_submit or self.__outstanding_futures > 0)

    def __event_timer(self, *args):
        if not self.__work_remains():
            self.__broker.reactor_stop()
        while self.__jobspecs_to_submit:
            jobspec, user_future = self.__jobspecs_to_submit.popleft()
            flux.job.submit_async(self.__broker, jobspec, waitable=True).then(
                self.__get_jobid_from_submission_future, user_future
            )
            self.__outstanding_futures += 1

    def __get_jobid_from_submission_future(self, submission_future, user_future):
        """Callback invoked when a jobid is ready for a submitted jobspec."""
        jobid = flux.job.submit_get_id(submission_future)
        user_future.set_jobid(jobid)
        flux.job.wait_async(self.__broker, jobid).then(self.__complete_user_future, future)

    @classmethod
    def __complete_user_future(cls, wait_future, user_future):
        """Callback invoked when a job has completed."""
        result = wait_future.get_status()
        self.__outstanding_futures -= 1
        if result.success:
            user_future.set_result(None)
        else:
            user_future.set_exception(
                JobFailure(f"Job exited abnormally: {result.errstr}")
            )


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

    def __init__(self, thread_name_prefix="", poll_frequency=0.25, broker_args=(), broker_kwargs={}):
        self._submission_queue = collections.deque()
        self._jobid_future_pairs = collections.deque()
        self._shutdown_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        thread_name_prefix = (
            thread_name_prefix or f"{type(self).__name__}-{self._counter()}"
        )
        self._executor_thread = FluxExecutorThread(
            self._shutdown_event,
            self._submission_queue,
            poll_frequency,
            broker_args,
            broker_kwargs,
            name=(thread_name_prefix + "-0"),
        )
        self._executor_thread.start()

    def shutdown(self, wait=True, *, cancel_futures=False):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.


        :param wait: If True then shutdown will not return until all running
                futures have finished executing and the resources used by the
                executor have been reclaimed.
        :param cancel_futures: offered only
        """
        with self._shutdown_lock:
            self._shutdown_event.set()
        if wait:
            self._executor_thread.join()

    def submit(self, jobspec):
        """Submit a jobspec to Flux and return a future."""
        with self._shutdown_lock:
            if self._shutdown_event.is_set():
                raise RuntimeError("cannot schedule new futures after shutdown")
            fut = FluxExecutorFuture()
            fut.set_running_or_notify_cancel()
            self._submission_queue.append((jobspec, fut))
            return fut

    def map(self, *args, **kwargs):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=True)
        return False


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
