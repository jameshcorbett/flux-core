#!/usr/bin/env python3
###############################################################
# Copyright 2020 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################

import unittest
import functools
import threading
import concurrent.futures

from flux.job import JobspecV1, FluxExecutor, FluxExecutorFuture, JobFailure

from pycotap import TAPTestRunner


def set_event(event, fut):
    event.set()


class TestFluxExecutor(unittest.TestCase):
    def test_submit(self):
        with FluxExecutor() as executor:
            jobspec = JobspecV1.from_command(["true"])
            futures = [executor.submit(jobspec) for _ in range(5)]
            for fut in futures:
                self.assertIsNone(fut.result())
                self.assertIsNone(fut.exception())

    def test_failed_submit(self):
        with FluxExecutor() as executor:
            jobspec = JobspecV1.from_command(["false"])
            futures = [executor.submit(jobspec) for _ in range(5)]
            for fut in futures:
                with self.assertRaises(JobFailure):
                    fut.result()
                self.assertIsInstance(fut.exception(), JobFailure)

    def test_cancel(self):
        with FluxExecutor() as executor:
            jobspec = JobspecV1.from_command(["false"])
            for _ in range(5):
                future = executor.submit(jobspec)
                if future.cancel():
                    self.assertFalse(future.running())
                    self.assertTrue(future.cancelled())
                with self.assertRaises(concurrent.futures.CancelledError):
                    future.result()

    def test_map(self):
        with FluxExecutor() as executor:
            with self.assertRaises(NotImplementedError):
                executor.map(print, [])

    def test_bad_jobspec(self):
        with FluxExecutor() as executor:
            jobspec = None  # not a valid jobspec
            futures = [executor.submit(jobspec) for _ in range(5)]
            for fut in futures:
                with self.assertRaises(OSError):
                    fut.result()
                self.assertIsInstance(fut.exception(), OSError)

    def test_submit_after_shutdown(self):
        executor = FluxExecutor()
        executor.shutdown(cancel_futures=True)
        with self.assertRaises(RuntimeError):
            executor.submit(JobspecV1.from_command(["true"]))

    def test_jobid(self):
        with FluxExecutor() as executor:
            jobspec = JobspecV1.from_command(["true"])
            futures = [executor.submit(jobspec) for _ in range(5)]
            events = [threading.Event() for _ in futures]
        for fut, event in zip(futures, events):
            self.assertFalse(event.is_set())
            fut.add_jobid_callback(functools.partial(set_event, event))
            jobid = fut.jobid()
            self.assertGreater(jobid, 0)
            self.assertTrue(event.is_set())

    def test_as_completed(self):
        with FluxExecutor() as executor:
            jobspec = JobspecV1.from_command(["true"])
            futures = [executor.submit(jobspec) for _ in range(5)]
            for fut in concurrent.futures.as_completed(futures):
                self.assertIsNone(fut.result(timeout=0))
                self.assertIsNone(fut.exception())

    def test_wait(self):
        with FluxExecutor() as executor:
            jobspec = JobspecV1.from_command(["false"])
            futures = [executor.submit(jobspec) for _ in range(5)]
            done, not_done = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            self._check_done(done)
            done, not_done = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_EXCEPTION
            )
            self._check_done(done)
            done, not_done = concurrent.futures.wait(futures)
            self._check_done(done)
            self.assertEqual(len(not_done), 0)

    def _check_done(self, done_futures):
        self.assertGreater(len(done_futures), 0)
        for fut in done_futures:
            self.assertIsInstance(fut.exception(timeout=0), JobFailure)


if __name__ == "__main__":
    unittest.main()


# vi: ts=4 sw=4 expandtab
