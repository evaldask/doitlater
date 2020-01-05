
# DoItLater
> *When you have to do something... later.*

A simple python library to schedule work in the future with ability to loop infinitely. Does not depend on any 3rd party libraries.

* [Installation](#installation)
* [Usage](#usage)
* [API](#api)
___
## Installation
The package can be simply installed using on of these methods:
* `pip`: `pip install doitlater`
* `poetry`: `poetry add doitlater`
* `poetry`: `poetry add git+https://github.com/evalkaz/doitlater.git`
---
## Usage
Simplest example:
```python
from datetime import datetime, timedelta
from doitlater import Later

later = Later()

@later.on(datetime(2021, 1, 1))
def say_hello():
    print("Happy new years in 2021!")

if __name__ == "__main__":
    later.do()
```

To call the same function every 10 seconds with 30 seconds cold start:
```python
@later.on(
    datetime.now() + timedelta(seconds=30),
    timedelta(seconds=10),
    loop=True
)
def repeatable_work():
    print("Hello every 10 seconds!")
```
The `later.on()` is stackable so this will work too (will be executed every 5 and every 7 days):
```python
@later.on(datetime(2021, 1, 1), timedelta(days=5))
@later.on(datetime(2021, 1, 1), timedelta(weeks=1))
def say_hello():
    print("Happy new years in 2021!")
```
You can also pass a list of `datetime` or `timedelta` (or a mixed one) when to execute the function:
```python
@later.on(datetime(2021, 1, 1), [
    datetime(2021, 2, 1),
    datetime(2021, 3, 1),
    timedelta(days=31)], loop=False)
def say_hello():
    print("Happy new years in 2021!")
```
If you want to stop executing the same function just return `False`:
```python
@later.on(datetime(2021, 1, 1), timedelta(seconds=5), loop=True)
def only_one_hello():
    print("This will appear only once!")
    return False
```
If you have multiple functions and one of them throws an exception but you don't want to stop the work, pass `ignore_errors=True` to `Later()` object:
```python
later = Later(ignore_errors=True)
```
By default, library will output logs on errors. To change logging, use [`dictConfig`](https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig):
```python
from datetime import datetime, timedelta
from doitlater import Later
from logging.config import dictConfig

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
            }
        },
        "handlers": {
            "default": {"class": "logging.StreamHandler", "formatter": "default",}
        },
        "root": {"level": "INFO", "handlers": ["default"]},
    }
)

later = Later()

@later.on(datetime(2021, 1, 1))
def say_hello():
    print("Happy new years in 2021!")

if __name__ == "__main__":
    later.do()
```
---
## API
`Later(workers, ignore_errors)` takes these parameters:
* `workers` - number of threads to use, defaults to maximum number of threads supported by CPU.
* `ignore_errors` - `False` will exit when one of the function throws an error, `True` - ignores exceptions and will resume the work.

`later.on(exactly, repeat, loop)` function takes these parameters:
* `exactly` - on which time perform the first function call.
* `repeat` - a single value or a list of `datetime`, `timedelta` or both on when to repeat the call. If `None` is passed the function will not be called again. Defaults to `None`.
* `loop` - whether repeat calling times from `repeat`. Defaults to `True`.

`later.do()` does not take any parameters.
