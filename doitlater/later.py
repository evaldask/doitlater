import logging
import multiprocessing
import threading
from datetime import datetime, timedelta
from functools import wraps
import queue
import time

from typing import Optional, Union, Iterable, Callable


class Work(object):
    def __init__(self, date: datetime, func: Callable, repeat: bool, loop: bool):
        self.date = date  # When execute the function.
        self.func = func  # Function which to execute.
        self.repeat = repeat  # Should function call be repeated.
        self.loop = loop  # Does repeat times are on loop.


class Worker(threading.Thread):
    def __init__(self, queue, ignore_errors, *args, **kwargs):
        self.__q = queue
        self.__ignore = ignore_errors
        self.__stop = False
        super().__init__(*args, **kwargs)

    def run(self):
        while not self.__stop:
            # Nothing to do anymore, quit.
            if self.__q.empty():
                return

            work = self.__q.get(block=False)

            # Wait until right time.
            to_wait = work.date - datetime.now()
            time.sleep(to_wait.total_seconds())

            # Call the function.
            try:
                res = work.func()
            except Exception as e:
                if not self.__ignore:
                    raise e
                res = None
            finally:
                self.__q.task_done()

            # The task indicated to stop, break the loop.
            if res is not None and res is False:
                break

            # If we have to repeat.
            if work.repeat:
                next_time = work.repeat.pop(0)
                # If this cycle loops.
                if work.loop:
                    work.repeat.append(next_time)
                work.date += next_time
                self.__q.put(work)

    def stop(self):
        self.__stop = True


class Later(object):
    def __init__(
        self,
        workers: Optional[int] = None,
        logging_level: int = logging.INFO,
        ignore_errors: bool = False,
    ):
        if not workers:
            workers = multiprocessing.cpu_count()

        self.__log_level = logging_level
        self.__queue = queue.Queue()
        self.__workers = [Worker(self.__queue, ignore_errors) for _ in range(workers)]

        self._args = dict()
        self.__last_func = None

    def on(
        self,
        exactly: datetime,
        repeat: Union[timedelta, Iterable[timedelta]] = None,
        loop: bool = True,
    ) -> Callable:

        if repeat and (isinstance(repeat, timedelta) or isinstance(repeat, datetime)):
            repeat = [repeat]

        # Normalize repeat list by converting it to timedelta type.
        if isinstance(repeat, Iterable):
            passed = exactly
            for i in range(1, len(repeat)):
                if isinstance(repeat[i], datetime):
                    repeat[i] -= passed

                if repeat[i].total_seconds() < 0:
                    raise ValueError("Repeat time is negative.")

                passed += repeat[i]

        def decorator(func):
            if not func:
                func = self.__last_func

            work = Work(exactly, func, repeat, loop)
            self.__queue.put(work)

            self.__last_func = func

        return decorator

    def do(self):
        try:
            for w in self.__workers:
                w.start()

            self.__queue.join()
        except Exception as e:
            for w in self.__workers:
                w.stop()
            raise e
