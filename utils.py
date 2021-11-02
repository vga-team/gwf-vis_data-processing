from datetime import date, datetime, timedelta
import errno
import os

SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24

def logExecutionTime(operationName=None):
    if not operationName:
        operationName = 'the operation'

    def wrapper(fn):
        def inner(*args, **kw):
            startTime = datetime.now()
            print(f'Started {operationName} at {startTime}.')
            result = fn(*args, **kw)
            endTime = datetime.now()
            print(f'Ended {operationName} at {endTime}.')
            timeElapsed = endTime - startTime
            print(
                f'Time elapsed for {operationName} is {timeElapsed.days} days and {timeElapsed.seconds} seconds and {timeElapsed.microseconds} microseconds.')
            return result
        return inner
    return wrapper


def writeFile(path: str, content: str):
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as e:  # Guard against race condition
            if e.errno != errno.EEXIST:
                raise
    with open(path, 'w') as file:
        file.write(content)


def generateLoopingRange(inputRange: range):
    loopingRange = range(
        inputRange.start, inputRange.stop + 1, inputRange.step)
    return loopingRange


def calculateDaysForTheYear(year: int):
    date1 = date(year, 1, 1)
    date2 = date(year + 1, 1, 1)
    delta = date2 - date1
    days = delta.days
    return days


def calculateActualDatetime(initialDatetime: datetime, delta: timedelta):
    return initialDatetime + delta
