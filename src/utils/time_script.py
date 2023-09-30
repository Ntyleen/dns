import time
from functools import wraps


def time_stamp(func):
    @wraps(func)
    def time_wrap(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        if elapsed_time < 60:
            print(f"Функция '{func.__name__}' выполнена за {elapsed_time:.4f} секунд")
        else:
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Функция '{func.__name__}' выполнена за {int(minutes)} минут {seconds:.4f} секунд")
        return result

    return time_wrap
