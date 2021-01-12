"""This module defines the FluxExecutor class."""

import threading
import collections
import concurrent.futures

import flux
import flux.job


class WaitingThread(threading.Thread):
    """Thread that waits for jobs to complete and fulfills futures."""

    def __init__(self, exit_event, future_queue, **kwargs):
        super().__init__(**kwargs)
        self.__flux_handle = flux.Flux()
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
            return flux.job.wait(self.__flux_handle)
        except:  # no waitable jobs
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

    def __init__(self, exit_event, jobspecs_to_submit, jobid_future_pairs, **kwargs):
        super().__init__(**kwargs)
        self.__exit_event = exit_event
        self.__jobspecs_to_submit = jobspecs_to_submit
        self.__jobid_future_pairs = jobid_future_pairs

    def run(self):
        """Loop indefinitely, submitting jobspecs and fetching jobids."""
        broker = flux.Flux()
        while not self.__exit_event.is_set():
            while self.__jobspecs_to_submit:
                jobspec, user_future = self.__jobspecs_to_submit.popleft()
                flux.job.submit_async(broker, jobspec, waitable=True).then(
                    self._get_jobid_from_submission_future, user_future
                )
            if (
                broker.reactor_run(
                    broker.get_reactor(), flux.constants.FLUX_REACTOR_NOWAIT
                )
                < 0
            ):
                msg = "reactor start failed"
                broker.fatal_error(msg)
                raise RuntimeError(msg)

    def _get_jobid_from_submission_future(self, submission_future, user_future):
        """Callback invoked when a jobid is ready for a submitted jobspec."""
        jobid = flux.job.submit_get_id(submission_future)
        self.__jobid_future_pairs.append((jobid, user_future))


class FluxExecutor:
    """Provides methods to submit jobs to Flux asynchronously.

    Heavily inspired by the ``concurrent.futures.Executor`` class.
    """

    def __init__(self):
        self._submission_queue = collections.deque()
        self._jobid_future_pairs = collections.deque()
        self._shutdown_event = threading.Event()
        self._submission_thread = SubmissionThread(
            self._shutdown_event,
            self._submission_queue,
            self._jobid_future_pairs,
            daemon=True,
        )
        self._submission_thread.start()
        self._waiting_thread = WaitingThread(
            self._shutdown_event, self._jobid_future_pairs, daemon=True,
        )
        self._waiting_thread.start()

    def shutdown(self):
        self._shutdown_event.set()

    def submit(self, jobspec):
        """Submit a jobspec to Flux and return a future."""
        fut = concurrent.futures.Future()
        fut.set_running_or_notify_cancel()
        self._submission_queue.append((jobspec, fut))
        return fut
