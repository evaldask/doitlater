import logging
from logging.config import dictConfig
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
                logging.debug("Executing %s.", work.func.__name__)
                res = work.func()
            except Exception as e:
                logging.error("%s threw an exception '%s'", work.func.__name__, e)
                res = None

                if not self.__ignore:
                    raise e
                else:
                    logging.info("Exception was ignored.")

            finally:
                logging.info("%s finished work.", work.func.__name__)
                self.__q.task_done()

            # The task indicated to stop, break the loop.
            if res is not None and res is False:
                logging.info(
                    "Stopping %s task because function returned false.",
                    work.func.__name__,
                )
                break

            # If we have to repeat.
            if work.repeat:
                next_time = work.repeat.pop(0)
                # If this cycle loops.
                if work.loop:
                    work.repeat.append(next_time)
                work.date += next_time
                self.__q.put(work)
                logging.debug(
                    "Put %s task in the queue. Next execution on %s.",
                    work.func.__name__,
                    work.date,
                )

    def stop(self):
        self.__stop = True


class Later(object):
    def __init__(
        self,
        workers: Optional[int] = None,
        ignore_errors: bool = False,
        logging_level: int = logging.INFO,
        logging_format: str = "",
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

            logging.info(
                "Put %s task in the queue. Will be executed on %s.",
                work.func.__name__,
                work.date,
            )
            self.__last_func = func

        return decorator

    def do(self):
        try:
            for w in self.__workers:
                w.start()

            logging.debug("Started all workers.")
            self.__queue.join()

        except Exception as e:
            logging.debug("Stopping all workers.")
            for w in self.__workers:
                w.stop()
            raise e


dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
        "handlers": {
            "default": {
                "level": "INFO",
                "formatter": "basic",
                "class": "logging.StreamHandler",
            }
        },
        "formatters": {
            "basic": {"format": "[%(asctime)s][%(levelname)s] %(name)s: %(message)s"}
        },
        "loggers": {"": {"level": "ERROR", "handlers": ["default"]}},
    }
)
