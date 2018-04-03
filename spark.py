#import findspark
#findspark.init()

import os
import shutil

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext

#SparkContext.setSystemProperty('spark.executor.memory', '8g')
#SparkContext.setSystemProperty('spark.python.worker.memory', '8g')
#SparkContext.setSystemProperty('spark.driver.memory', '16g')
#ss = SparkSession\
#     .builder\
#     .master("local[20]")\
#     .appName("testing")\
#     .config('spark.executor.memory', '8G')\
#     .config('spark.worker.memory', '8G')\
#     .config('spark.driver.memory', '8G')\
#     .getOrCreate()
#     .set('spark.driver.memory', '45G')
#     .set('spark.driver.maxResultSize', '10G')  
#conf = SparkConf().setAppName("App")
#conf = (conf.setMaster('local[*]')
#        .set('spark.executor.memory', '4G')
#        .set('spark.driver.memory', '45G')
#        .set('spark.driver.maxResultSize', '10G'))

### old spark utils

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.ml.feature import StringIndexer, VectorAssembler

from util.misc import named_of_indexed, tab
from util.jvm import class_name

#from py4j.java_collections import JavaIterator

class ByKeyAndIndex(object):
    """ A list of items that can be accessed either by index or by a
    key."""

    __slots__ = ['keys_by_index', 'items_by_index', 'items_by_key']

    def __init__(self, items_by_index, keys=None):
        """ If the keys for the items are not provided, the items
        themselves become the keys."""

        self.keys_by_index = keys
        self.items_by_index = items_by_index
        self.items_by_key = named_of_indexed(self.items_by_index, keys=keys)

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.items_by_index[index]
        else:
            return self.items_by_key[str(index)]

class ValueMapper(object):
    """ Holds the information about column names of a dataset, and the
    values of a dataset that were converted to doubles in order to use
    various spark models. """

    __slots__ = ['target_idx', 'target_name', 'columns', 'types', 'values']

    def __init__(self, target_idx, columns_by_idx, types_by_idx, values_by_idx):
        self.target_idx = target_idx
        self.target_name = str(columns_by_idx[target_idx])

        self.columns = ByKeyAndIndex(columns_by_idx)
        self.types = ByKeyAndIndex(types_by_idx, keys=columns_by_idx)
        self.values = ByKeyAndIndex(values_by_idx, keys=columns_by_idx)

class Spark(object):
    """ Store the various elements of of a spark setup and context."""

    __slots__ = ['conf', 'spark', 'sql', "session", "jvm", "master"]

    def __init__(self, master="local[*]"):
        """ Create only the spark configuration at first. """

        self.conf = SparkConf().setMaster(master)
        self.master = master

        self.spark = None
        self.sql = None

    def start(self):
        """ Initialize the rest of the stuff, assuming the
        configuration has been filled in."""

        self.session = SparkSession\
            .builder\
            .master(self.master)\
            .appName("testing")\
            .getOrCreate()
        self.spark = self.session.sparkContext
        self.sql = SQLContext(self.spark)
        self.jvm = SparkContext._jvm

        print("spark version = " + self.spark.version)

        for row in (self.spark._conf.getAll()):
            print (row)

        return self

    def __getattr__(self, name):
        """ For attributes not provided by this Spark object, try to
        find the attribute in one of the three objects from spark
        itself."""

        component = None

        if hasattr(self.conf, name):
            component = "conf"
        elif hasattr(self.spark, name):
            component = "spark"
        elif hasattr(self.sql, name):
            component = "sql"
        else:
            raise ValueError("no such attribute: %s" % name)

        subobject = getattr(self, component)
        field = getattr(subobject, name)

        def enclosed(*args, **kwargs):
            """ If the field is a method, and it returns one of the 3
            subcomponents, retrofit it so that it returns this whole
            class after updating the subcomponent. """

            ret = field(*args, **kwargs)
            if type(ret) is type(subobject):
                setattr(self, component, ret)
                return self
            else: return ret

        if callable(field):
            return enclosed
        else:
            return field

    def load_csv(self, filename, sep=","):
        return self.session.read.csv(filename,\
                           inferSchema=True,\
                           sep=sep,\
                           header=True,\
                           )

    def save_csv(self, filename, df, sep=","):
        if (os.path.exists(filename)): shutil.rmtree(filename)
        return df.write.csv(filename,\
                            sep=sep,\
                            header=True,\
                            )

    def read_csv(self, filename,
                 header='true', encoding='UTF-8', inferschema='true'):
        """ Read a CSV file into a dataframe. """

        return self.sql.read.format("com.databricks.spark.csv")\
                    .options(header=header,
                             encoding=encoding,
                             inferschema=inferschema
                            ).load(filename)

def assemble_features(dataframe, columns,
                      output_column="features", exclude=None):
    """ Collect the set of given features into a vector column to use
    for training."""

    if exclude is None:
        exclude = []

    assembler = VectorAssembler(inputCols=[col for col in columns
                                           if col not in exclude],
                                outputCol=output_column)

    return assembler.transform(dataframe)

def index_nominals(dataframe, renamer=lambda string: u"indexed_%s" % string):
    """ Create indexed versions of nominal features in the given
    dataframe."""

    all_cols = dataframe.columns

    schema = dataframe.schema
    names_by_idx = [str(name) for name in schema.names]
    types_by_idx = [field.dataType for field in schema.fields]
    labels_by_idx = [[] for idx in range(len(all_cols))]

    dataframe_indexing = dataframe

    # The new (or old if not indexed) column names.
    columns = []

    # Fit and apply a sequence of nominal feature indexer.
    for idx, col in enumerate(all_cols):
        if types_by_idx[idx] is StringType():
            # Encode nominal features into doubles.
            indexer = StringIndexer(inputCol=col,
                                    outputCol=renamer(col)
                                    ).fit(dataframe_indexing)
            labels_by_idx[idx] = indexer.labels
            dataframe_indexing = indexer.transform(dataframe_indexing)
            columns.append(renamer(col))
        else:
            labels_by_idx[idx] = []
            columns.append(col)

    # Create the object that holds the information necessary to get
    # column and value names for the various converted features and
    # values.
    namer = ValueMapper(0, names_by_idx, types_by_idx, labels_by_idx)

    return dataframe_indexing, columns, namer

def pylist_of_scala_list(l):
    """
    Convert scala type :: (the cons in a lisp-like language) to a
    python list. Py4j has issues with these for some reason.
    """

    typ = class_name(l)

    if typ == "Nil$":
        return []
    elif typ == "$colon$colon":
        return [l.head()] + pylist_of_scala_list(l.tail())

def string_of_mllib_split(split, namer=None):
    """
    String representation of a scala spark mllib (old API) Split:
    """

    typ = class_name(split)

    if typ != "Split":
        raise ValueError("unknown class: %s" % typ)

    ret = u""
    if namer is None:
        ret = u"f%d " % split.feature()
    else:
        ret = u"%s " % namer.columns[split.feature()]

    stype = split.featureType().toString()

    if stype == "Categorical":
        cats = pylist_of_scala_list(split.categories())
        ret += u"∈ {%s}" % u",".join([str(c) for c in cats])
    elif stype == "Continuous":
        ret += u"≤ %f" % split.threshold()

    return ret

def string_of_mllib_node(node, namer=None):
    """
    String representation of a scala spark mllib (old API) Node:
    """

    typ = class_name(node)

    if typ != "Node":
        raise ValueError("unknown class: %s" % typ)

    predict = node.predict().predict()

    info = u""
    if namer is not None:
        info = u"prediction: %s = %s" % (namer.target_name, str(predict))
    else:
        info = u"prediction: output = %s" % str(predict)

    ret = u""

    if not node.isLeaf():
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.InternalNode
        ret += u"if %s (%s) then\n" % \
               (string_of_mllib_split(
                  node.split().get(),
                  namer),
                info) + \
               tab(string_of_mllib_node(node.leftNode().get(), namer)) +  u"\n" + \
               u"else\n" + \
               tab(string_of_mllib_node(node.rightNode().get(), namer))

    else:
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.LeafNode
        if namer is None:
            ret += u"output = %s" % str(predict)
        else:
            ret += u"%s = %s" % (namer.target_name,
                                 namer.values
                                 [namer.target_idx]
                                 [predict])

    return ret

def string_of_split(split, namer=None):
    """
    String representation of a scala spark Split:
    https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.Split
    """

    typ = class_name(split)

    ret = u""
    if namer is None:
        ret = u"f%d" % split.featureIndex()
    else:
        ret = u"%s " % namer.columns[split.featureIndex()]

    idx = split.featureIndex() + 1

    if typ == "CategoricalSplit":
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.CategoricalSplit

        if namer is None:
            ret += "∈ {%s}" % u",".join([str(c) for c in split.leftCategories])
        else:
            ret += "∈ {%s}" % u",".join([namer.values[idx][int(cat)]
                                         for cat in split.leftCategories()])
    elif typ == "ContinuousSpit":
        ret += u"≤ %f" % split.threshold()
    else:
        ret += u"unknown split: % s" % str(split)

    return ret

def string_of_node(node, namer=None):
    """
    String representation of a scala spark Node:
    https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.Node
    """

    typ = class_name(node)

    info = u""

    if namer is None:
        info = u"prediction: output = %d" % node.prediction()
    else:
        info = u"prediction: %s = %s" % (namer.target_name,
                                         namer.values
                                         [namer.target_idx]
                                         [node.prediction()])

    ret = u""

    if typ == "InternalNode":
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.InternalNode
        ret += u"if %s (%s) then\n" % (string_of_split(node.split(), namer),
                                       info) + \
               tab(string_of_node(node.leftChild(), namer)) +  u"\n" + \
               u"else\n" + \
               tab(string_of_node(node.rightChild(), namer))

    elif typ == "LeafNode":
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.LeafNode
        if namer is None:
            ret += u"output = %d" % int(node.prediction())
        else:
            ret += u"%s = %s" % (namer.target_name,
                                 namer.values
                                 [namer.target_idx]
                                 [int(node.prediction())])
    else:
        ret += u"unknown class"

    return ret
