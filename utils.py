import time


def logExecutionTime(operationName=None):
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


def writeFile(path: str, content: str):
    file = open(path, 'w')
    file.write(content)
    file.close()
