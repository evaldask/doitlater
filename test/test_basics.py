import unittest
import time
from datetime import datetime, timedelta

from doitlater import Later


class TestBasics(unittest.TestCase):
    def test_single_future(self):
        later = Later()
        later._args["performed"] = False

        @later.on(datetime.now() + timedelta(seconds=1))
        def do_smth():
            later._args["performed"] = True

        later.do()

        self.assertTrue(later._args["performed"])

    def test_repeatable_future_two(self):
        later = Later()
        later._args["performed"] = 0

        @later.on(
            datetime.now() + timedelta(seconds=1), timedelta(seconds=1), loop=False,
        )
        def do_smth():
            later._args["performed"] += 1

        later.do()

        self.assertEqual(later._args["performed"], 2)

    def test_repeatable_future_three(self):
        later = Later()
        later._args["performed"] = 0

        @later.on(
            datetime.now() + timedelta(seconds=1),
            [timedelta(seconds=1), timedelta(seconds=1)],
            loop=False,
        )
        def do_smth():
            later._args["performed"] += 1

        later.do()

        self.assertEqual(later._args["performed"], 3)

    def test_repeatable_future_loop(self):
        later = Later()
        later._args["performed"] = 0

        @later.on(
            datetime.now() + timedelta(seconds=1), timedelta(seconds=1), loop=True,
        )
        def do_smth():
            later._args["performed"] += 1
            if later._args["performed"] >= 5:
                raise OverflowError("Raise false error.")

        later.do()

        self.assertEqual(later._args["performed"], 5)
        self.assertRaises(OverflowError)

    def test_repeatable_future_loop_errors(self):
        later = Later(ignore_errors=True)
        later._args["performed"] = 0

        @later.on(
            datetime.now() + timedelta(seconds=1), timedelta(seconds=1), loop=True
        )
        def do_smth():
            later._args["performed"] += 1
            if later._args["performed"] == 3:
                raise OverflowError("Raise false error.")
            if later._args["performed"] == 4:
                return False

        later.do()

        self.assertEqual(later._args["performed"], 4)

    def test_stackable_future(self):
        later = Later()
        later._args["performed"] = 0

        @later.on(datetime.now() + timedelta(seconds=1))
        @later.on(datetime.now() + timedelta(seconds=2))
        @later.on(datetime.now() + timedelta(seconds=3))
        def do_smth_else():
            later._args["performed"] += 1

        later.do()

        self.assertEqual(later._args["performed"], 3)

    def test_stackable_future_sleep(self):
        later = Later()
        later._args["performed"] = 0
        later._args["current_time"] = datetime.now()
        later._args["passed"] = 0

        @later.on(
            datetime.now() + timedelta(seconds=1),
            [timedelta(seconds=1), timedelta(seconds=1)],
            loop=False,
        )
        def do_smth():
            now = datetime.now()
            later._args["passed"] += (now - later._args["current_time"]).total_seconds()
            later._args["current_time"] = now

        later.do()

        self.assertAlmostEqual(later._args["passed"], 3, places=2)


if __name__ == "__main__":
    unittest.main()
