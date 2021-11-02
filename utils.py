from datetime import datetime


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
    file = open(path, 'w')
    file.write(content)
    file.close()
