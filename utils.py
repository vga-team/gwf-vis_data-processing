import time


def logTime(operationName=None):
    if not operationName:
        operationName = 'the operation'

    def wrapper(fn):
        def inner(*args, **kw):
            startTime = time.time()
            result = fn(*args, **kw)
            endTime = time.time()
            print(
                f'Time elapsed for {operationName} is {endTime - startTime} seconds.')
            return result
        return inner
    return wrapper
