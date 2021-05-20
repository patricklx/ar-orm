__author__ = 'Patrick'

from typing import Callable, List, TypeVar, Type


def find_by(array, by, value):
    for i in array:
        if i[by] == value:
            return i
    return None


T = TypeVar('T')


def find(array: List[T], _lambda: Callable[[T], bool]) -> T:
    for i in array:
        if _lambda(i):
            return i
    return None
