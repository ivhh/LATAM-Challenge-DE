import io
from memory_profiler import profile as mem_profile
import cProfile
import pstats
from typing import Callable, Any


def mem_profiler(mem_map: dict, tag: str) -> Callable[..., Any]:
    """
    A decorator factory that profiles the memory usage of a given function.

    Args:
        mem_map (dict): A dictionary to store the memory profile results.
        tag (str): A tag to identify the memory profile result in the mem_map.

    Returns:
        Callable[..., Any]: A decorator that profiles the memory usage of a function.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args, **kwargs):
            with io.StringIO() as buffer:
                result = None

                @mem_profile(stream=buffer)
                def run_func():
                    result = func(*args, **kwargs)

                run_func()
                mem_map[tag] = buffer.getvalue()
            return result

        return wrapper

    return decorator


def time_profiler(time_map: dict, tag: str) -> Callable[..., Any]:
    """
    A decorator factory that profiles the execution time of a given function.

    Args:
        time_map (dict): A dictionary to store the profiling results.
        tag (str): A tag to identify the profiling result in the time_map.

    Returns:
        Callable[..., Any]: A decorator that profiles the execution time of a function.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args, **kwargs):
            with cProfile.Profile() as pr:
                result = func(*args, **kwargs)
                string_io = io.StringIO()
                pstats.Stats(pr, stream=string_io).strip_dirs().sort_stats("cumulative").print_stats()
                time_map[tag] = string_io.getvalue()
            return result

        return wrapper

    return decorator
