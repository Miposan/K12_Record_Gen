import time
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def timer(prefix="", print_time=True):
    def decorator(func):
        nonlocal prefix
        if prefix == '':
            prefix = func.__name__
        else: 
            prefix = f"{prefix}({func.__name__})"
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            if print_time:
                print(f"{prefix} Time elapsed: {elapsed_time}")
            return result
        return wrapper
    return decorator