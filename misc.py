import os
import pickle
import math
import argparse

# pair accessors
def _0(n): return n[0]
def _1(n): return n[1]
def _2(n): return n[2]
def _3(n): return n[3]
def _4(n): return n[4]
def _5(n): return n[5]

# print and flush
def printme(s): print(s, end='', flush=True)

def load(filename):
    with open(filename, 'rb') as file:
        return pickle.load(file)
    
# load a value from a pickle if it exists, otherwise use the default
def load_or_new(filename, default):
    if (os.path.exists(filename)):
        return load(filename)
    else:
        return save(filename, default)

# save a value to a pickle
def save(filename, obj):
    with open(filename, 'wb') as file:
        pickle.dump(obj, file)
    return obj

# maths

def lg(d):
    return math.log(d,2.0)


# strings

def tab(string):
    """
    Prepend each line in the given string with 2 spaces.
    """

    return u"\n".join([u"  " + l for l in string.split(u"\n")])

# unsorted

class IdentityDictionary(dict):
    """ A dictionary that always returns the key when asked for item
    at key."""
    def __getitem__(self, key):
        return key

def named_of_indexed(items, keys=None):
    """
    Given a list of items, create a dictionary that maps their names
    (their names themselves or their key in the optional keys
    argument) to the item. Used to convert lists that index by integer
    to dictionaries that index by name.
    """

    ret = {}
    for idx, item in enumerate(items):
        item = items[idx]
        if keys is not None:
            ret[str(keys[idx])] = item
        else:
            ret[str(item)] = item
    return ret

# command line parsing

def get_args_string():
    """ Command line parser for a single string argument. """
    parser = argparse.ArgumentParser()

    parser.add_argument("string", type=str)

    args = parser.parse_args()

    return args.string

def get_args_2strings():
    """ Command line parser for a single string argument. """
    parser = argparse.ArgumentParser()

    parser.add_argument("string1", type=str)
    parser.add_argument("string2", type=str)

    args = parser.parse_args()

    return (args.string1, args.string2)
