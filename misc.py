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


# useful mixins

class withprintedvars(withlocals):
    def __init__(self):
        object.__init__(self)

    def __str__(self): return str(self.__dict__)
    def __repr__(self): return repr(self.__dict__)

# print and flush
def printme(s): print(s, end='', flush=True)

def load(filename, pickler=pickle):
    print("load %s via %s" % (filename, pickler.__name__))
    with open(filename, 'rb') as file:
        return pickler.load(file)
    
# load a value from a pickle if it exists, otherwise use the default
def load_or_new(filename, default, pickler=pickle):
    if (os.path.exists(filename)):
        return load(filename, pickler=pickler)
    else:
        return save(filename, default, pickler=pickler)

# save a value to a pickle
def save(filename, obj, pickler=pickle):
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

# collections

class ArrayDict(dict):
    """ Combination of array and dictionary, can be accessed either way. """

    #__slots__ = ['keys_by_index', 'items_by_index', 'items_by_key']

    def __init__(self, items_by_index, keys=None, **kwargs):
        """ If the keys for the items are not provided, the items
        themselves become the keys."""

        if keys is None:
            keys = items_by_index
 
        dict.__init__(self, named_of_indexed(items_by_index, keys=keys), **kwargs)
        
        self.keys_by_index  = keys
        self.items_by_index = items_by_index

    def __setitem__(self, index, v):
        if isinstance(index, int):
            self.items_by_index[index] = v
        else:
            dict.__setitem__(self, str(index), v)
        
    def __getitem__(self, index):
        if isinstance(index, int):
            return self.items_by_index[index]
        else:
            return dict.__getitem__(self, str(index))

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
