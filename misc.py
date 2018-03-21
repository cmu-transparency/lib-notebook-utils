import os
import pickle

# pair accessors
def _0(n): return n[0]
def _1(n): return n[1]
def _2(n): return n[2]
def _3(n): return n[3]
def _4(n): return n[4]
def _5(n): return n[5]

# print and flush
def printme(s): print(s, end='', flush=True)

# load a value from a pickle if it exists, otherwise use the default
def load_or_new(filename, default):
    if (os.path.exists(filename)):
        with open(filename, 'rb') as file:
            return pickle.load(file)
    else:
        return save(filename, default)

# save a value to a pickle
def save(filename, obj):
    with open(filename, 'wb') as file:
        pickle.dump(obj, file)
    return obj

__all__ = ["_0", "_1", "_2", "_3", "_4", "_5", "printme", "load_or_new", "save"]
