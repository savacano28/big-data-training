#!/usr/bin/env python
# coding: utf-8

# Before you turn this problem in, make sure everything runs as expected. First, **restart the kernel** (in the menubar, select Kernel$\rightarrow$Restart) and then **run all cells** (in the menubar, select Cell$\rightarrow$Run All).
# 
# Make sure you fill in any place that says `YOUR CODE HERE` or "YOUR ANSWER HERE", as well as your name and collaborators below:

# In[1]:


NAME = "Stephanya CASANOVA MARROQUIN"
COLLABORATORS = ""


# ---

# ---
# # First steps into Spark
# 
# In this notebook, we will launch our very first Spark code.
# 
# ![Spark logo](http://spark.apache.org/images/spark-logo-trademark.png)
# 
# [Apache Spark](http://spark.apache.org/) is a cluster computing engine designed to be __fast__ and __general-purpose__, making it the ideal choice for processing of large datasets. It answers those two points with __efficient data sharing__ accross computations.
# <hr/>
# The past years have seen a major changes in computing systems, as growing data volumes required more and more applications to scale out to large clusters. To solve this problem, a wide range of new programming models have been designed to manage multiple types of computations in a distributed fashion, without having people learn too much about distributed systems. Those programming models would need to deal with _parallelism, fault-tolerance and resource sharing_ for us.
# 
# [Google's MapReduce](https://en.wikipedia.org/wiki/MapReduce) presented a simple and general model for batch processing, which handles faults and parallelism easily. Unfortunately the programming model is not adapted for other types of workloads, and multiple specialized systems were born to answer a specific need in a distributed way. 
# * Iterative : Giraph
# * Interactive : Impala, Piccolo, Greenplum
# * Streaming : Storm, Millwheel
# 
# The initial goal of Apache Spark is to try and unify all of the workloads for generality purposes. [Matei Zaharia](https://cs.stanford.edu/~matei/) in his [PhD dissertation](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2014/EECS-2014-12.pdf) suggests that most of the data flow models that required a specialized system needed _efficient data sharing_ accross computations:
# * Iterative algorithms like PageRank or K-Means need to make multiple passes over the same dataset
# * Interactive data mining often requires running multiple ad-hoc queries on the same subset of data
# * Streaming applications need to maintain and share state over time.
# 
# He then proposes to create a new abstraction that gives its users direct control over data sharing, something that other specialized systems would have built-in for their specific needs. The abstraction is implemented inside a new engine that is today called Apache Spark. The engine makes it possible to support more types of computations than with the original MapReduce in a more efficient way, including interactive queries and stream processing. 

# ---
# ## Prerequisites
# 
# Before running Spark code, we need to start a SparkContext instance. The following block will be common to every notebook so you can run your code.
# 
# While your SparkContext is running, you can hit `http://localhost:4040` to get an overview of your Spark local cluster and all operations ongoing.

# In[2]:


from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('lecture-lyon2').setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)
sc


# In[3]:


# Import other important libraries

from pyspark.rdd import RDD


# In[4]:


filePath = 'FL_insurance_sample.csv'


# ---
# ## Part A - Your first RDDs
# 
# In this chapter, we are going to introduce Spark's core abstraction for working with data in a distributed and resilient way : the <text style="color:red;">resilient distributed dataset</text>, or <text style="color:red;">RDD</text>. Under the hood, Spark automatically performs the distribution of RDDs and its processing around the cluster, so we can focus on our code and not on distributed processing problems, such as the handling of data locality or resiliency in case of node failure.
# 
# A RDD consists of a collection of elements partitioned accross the nodes of a cluster of machines that can be operated on in parallel. In Spark, work is expressed by the creation and transformation of RDDs using Spark operators.
# 
# <text style="color:red;">Note</text> : RDD is the core data structure to Spark, but the style of programming we are studying in this lesson is considered the _lowest-level API_ for Spark. The Spark community is pushing the use of Structured programming with Dataframes/Datasets instead, an optimized interface for working with structured and semi-structured data, which we will learn later. Understanding RDDs is still important because it teaches you how Spark works under the hood and will serve you to understand and optimize your application when deployed into production.
# 
# There are two ways to create RDDs: parallelizing an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

# ## Question
# 
# Generate a RDD from a Python array with the `parallelize` method.

# In[5]:


n_array=[]
for i in range (0,3):
    n_array.append(i+1)
    
print(n_array)


# In[6]:


def rdd_from_list(sc, n):
    """
    Return a RDD consisting of elements from 1 to n. 
    For now we assume we will always get n > 1, no need to test for the exception nor raise an Exception.
    """
    # YOUR CODE HERE
    n_array=[]
    for i in range(0, n):
        n_array.append(i+1)
    return sc.parallelize(n_array)


# In[7]:


"""
Graded cell

1 point
"""
# collect() method returns all elements in a RDD to the driver as a local list
print(rdd_from_list(sc, 10).collect())

result_rdd = rdd_from_list(sc, 3)

assert isinstance(result_rdd, RDD)
assert result_rdd.collect() == [1, 2, 3]


# ## Question
# 
# Generate a RDD from a file with the `textFile()` method.

# In[8]:


sc.textFile(filePath).first()


# In[9]:


def load_file_to_rdd(sc, path):
    """
    Create a RDD by loading an external file. We don't expect any formatting nor processing here.
    You don't need to raise an exception if the file does not exist.
    
    1 point
    """
    # YOUR CODE HERE
    return sc.textFile(path)


# In[10]:


"""
Graded cell

1 point
"""
result_rdd = load_file_to_rdd(sc, filePath)

assert isinstance(result_rdd, RDD)
assert result_rdd.take(1)[0] == 'policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity'


# ---
# ## Part B - Classic Spark operations
# 
# ### Operations
# 
# RDDs have two sets of parallel operations:
# 
# * transformations : which return pointers to new RDDs without computing them, it rather waits for an action to compute itself.
# * actions : which return values to the driver after running the computation. The `collect()` funcion is an operation which retrieves all elements of the distributed RDD to the driver.
# 
# RDD transformations are _lazy_ in a sense they do not compute their results immediately.
# 
# The following exercises study the usage of the most common Spark RDD operations.

# ### .map() and flatMap() transformation
# 
# The `.map(function)` applies the function given in argument to each of the elements inside the RDD. 
# 
# The `.flatMap(function)` applies the function given in argument to each of the elements inside the RDD, then flattens the list so that there are no more nested elements inside it. 

# # Question 1
# 
# Suppose we have a RDD containing only lists of 2 elements :
# 
# ```
# matrix = [[1,3], [2,5], [8,9]]
# matrix_rdd = sc.parallelize(matrix)
# ```
# 
# This data structure is reminiscent of a matrix.
# 
# Create an operation `.op1()` which multiplies the first column (or first coordinate of each element) of the matrix by 2, and removes 3 to the second column (second coordinate).

# In[11]:


sc.parallelize([[1,3], [2,5], [8,9]]).map(lambda row: [row[0]*2, row[1]-3]).collect()


# In[12]:


def op1(sc, mat):
    """
    Multiply the first coordinate by 2, remove 3 to the second
    """
    # YOUR CODE HERE
    return mat.map(lambda row: [row[0]*2, row[1]-3])


# In[13]:


"""
Graded cell

1 point
"""
matrix = [[1,3], [2,5], [8,9]]
matrix_rdd = sc.parallelize(matrix)
result_rdd = op1(sc, matrix_rdd)

assert isinstance(result_rdd, RDD)
assert result_rdd.collect() == [[2, 0], [4, 2], [16, 6]]


# # Question 2
# 
# Suppose we have a RDD containing sentences :
# 
# ```
# sentences_rdd = sc.parallelize(['Hi everybody', 'My name is Fanilo', 'and your name is Antoine everybody'])
# ```
# 
# Create an operation `.op2()` which returns all the words in the rdd, after splitting each sentence by the whitespace character.

# In[14]:


sc.parallelize(['Hi everybody', 'My name is Savacano', 'and I happy to be here']).flatMap(lambda ligne: ligne.split(" ")).collect()


# In[15]:


def op2(sc, sentences):
    """
    Return all words contained in the sentences.    
    """
    # YOUR CODE HERE
    return sentences.flatMap(lambda ligne: ligne.split(" "))


# In[16]:


"""
Graded cell

1 point
"""
sentences_rdd = sc.parallelize(['Hi everybody', 'My name is Fanilo', 'and your name is Antoine everybody'])
result_rdd = op2(sc, sentences_rdd)

assert isinstance(result_rdd, RDD)
assert result_rdd.collect() == ['Hi', 'everybody', 'My', 'name', 'is', 'Fanilo', 'and', 'your', 'name', 'is', 'Antoine', 'everybody']


# ### .filter() transformation
# 
# The `.filter(function)` transformation let's us filter elements verify a certain function.
# 
# # Question 3
# 
# Suppose we have a RDD containing numbers.
# 
# Create an operation `.op3()` which returns all the odd numbers.

# In[17]:


sc.parallelize(range(20)).filter(lambda num: num%2).collect()


# In[18]:


def op3(sc, numbers):
    """
    Return all numbers contained in the RDD that are odd.    
    """
    # YOUR CODE HERE
    return numbers.filter(lambda num: num%2)


# In[19]:


"""
Graded cell

1 point
"""
numbers = [1,2,3,4,5,6,7,8,9]
numbers_rdd = sc.parallelize(numbers)
result_rdd = op3(sc, numbers_rdd)

assert isinstance(result_rdd, RDD)
assert result_rdd.collect() == [1,3,5,7,9]


# ### .reduce() operation
# 
# The `.reduce(function)` transformation reduces all elements of the RDD into one using a specific method.
# 
# Do take note that, as in the Hadoop ecosystem, the function used to reduce the dataset should be associative and commutative.
# 
# # Question 4
# 
# Suppose we have a RDD containing numbers.
# 
# Create an operation `.op4()` which returns the sum of all squared odd numbers in the RDD, using the `.reduce()` operation.
# 
# _Hint: now's a good time to tell you that chaining transformations is possible..._

# In[20]:


sc.parallelize(range(100)).filter(lambda num: num%2).map(lambda num: num**2).reduce(lambda x,y: x+y)


# In[21]:


def op4(sc, numbers):
    """
    Return the sum of all squared odd numbers.   
    """
    # YOUR CODE HERE
    return numbers.filter(lambda num: num%2).map(lambda num: num**2).reduce(lambda x,y: x+y)


# In[22]:


"""
Graded cell

1 point
"""
numbers = range(100)
numbers_rdd = sc.parallelize(numbers)
result = op4(sc, numbers_rdd)

assert result == 166650


# ---
# ## Part C - Paired RDDs
# 
# If you recall the classic MapReduce paradigm, you were dealing with key/value pairs to reduce your data in a distributed manner. We define a pair as a tuple of two elements, the first element being the key and the second the value.
# 
# Key/value pairs are good for solving many problems efficiently in a parallel fashion so let us delve into them.
# 
# ```
# pairs = [('b', 3), ('d', 4), ('a', 6), ('f', 1), ('e', 2)]
# pairs_rdd = sc.parallelize(pairs)
# ```

# ### reduceByKey
# 
# The `.reduceByKey()` method works in a similar way to the `.reduce()`, but it performs a reduction on a key-by-key basis.
# 
# # Question
# 
# Time for the classic Hello world question !

# In[23]:


sc.parallelize(['Hi everybody', 'My name is Savacano', 'and I dont have a second name', 'but everybody calls me nia']).flatMap(lambda row:row.split(" ")).map(lambda word : (word,1)).reduceByKey(lambda x,y: x+y).collect()


# In[24]:


def wordcount(sc, sentences):
    """
    Given a RDD of sentences, return the wordcount, after splitting sentences per whitespace.
    """
    # YOUR CODE HERE
    return sentences.flatMap(lambda row:row.split(" ")).map(lambda word : (word,1)).reduceByKey(lambda x,y: x+y)


# In[25]:


"""
Graded cell

4 points
"""
sentences_rdd = sc.parallelize(['Hi everybody', 'My name is Fanilo', 'and your name is Antoine everybody'])
result_rdd = wordcount(sc, sentences_rdd)

assert isinstance(result_rdd, RDD)
assert result_rdd.collect() == [
    ('Hi', 1),
    ('everybody', 2),
    ('My', 1),
    ('name', 2),
    ('is', 2),
    ('Fanilo', 1),
    ('and', 1),
    ('your', 1),
    ('Antoine', 1)
]


# ### join
# 
# The `.join()` method joins two RDD of pairs together on their key element.
# 
# # Question
# 
# Let's give ourselves a `student-gender` RDD and a `student-grade` RDD. Compute the mean grade for each gender.
# 
# _Hint: this is a long exercise. Remember that the mean for a gender equals the sum of all grades divided by the count of the number of grades. You already know how to sum by key, and you can use the `countByKey()` function for returning a hashmap of gender to count of grades, then use that hashmap inside a map function to divide. Good luck !_

# In[26]:


genders_rdd = sc.parallelize([('1', 'M'), ('2', 'M'), ('3', 'F'), ('4', 'F'), ('5', 'F'), ('6', 'M')])
grades_rdd = sc.parallelize([('1', 5), ('2', 12), ('3', 7), ('4', 18), ('5', 9), ('6', 5)])
join_rdd= genders_rdd.join(grades_rdd).map(lambda row: row[1])
countsByKey = join_rdd.countByKey() 
join_rdd = join_rdd.reduceByKey(lambda x,y:x+y) 
join_rdd = join_rdd.map(lambda x: (x[0], x[1]/countsByKey[x[0]])).collect()
print(join_rdd)


# In[27]:


def mean_grade_per_gender(sc, genders, grades):
    """
    Given a RDD of studentID to grades and studentID to gender, compute mean grade for each gender returned as paired RDD.
    Assume all studentIDs are present in both RDDs, making inner join possible, no need to check that.
    """
    # YOUR CODE HERE
    join_rdd= genders_rdd.join(grades_rdd).map(lambda row: row[1])
    countsByKey = join_rdd.countByKey() 
    join_rdd = join_rdd.reduceByKey(lambda x,y:x+y) 
    return join_rdd.map(lambda x: (x[0], x[1]/countsByKey[x[0]]))


# In[28]:


"""
Graded cell

4 points
"""
genders_rdd = sc.parallelize([('1', 'M'), ('2', 'M'), ('3', 'F'), ('4', 'F'), ('5', 'F'), ('6', 'M')])
grades_rdd = sc.parallelize([('1', 5), ('2', 12), ('3', 7), ('4', 18), ('5', 9), ('6', 5)])

result_rdd = mean_grade_per_gender(sc, genders_rdd, grades_rdd)
assert isinstance(result_rdd, RDD)
assert result_rdd.collect() == [('M', 7.333333333333333), ('F', 11.333333333333334)]


# ---
# ## Part D - Operations on a file
# 
# We provide a `FL_insurance_sample.csv` file inside the `data` folder to use in our computations, it will be loaded through  `load_file_to_rdd()` you have previously implemented.

# ## Question
# 
# The first line of the CSV is the header, and it is annoying to have it mixed with the data. In the lower-level RDD API we need to write code to specifically filter that first line.
# 
# **Hint** : `rdd.zipwithindex()` is a useful function when you need to filter by position in a file _(though computationally expensive)_.

# In[29]:


sc.parallelize(['a', 'b', 'c', 'd']).zipWithIndex().collect()


# In[30]:


def filter_header(sc, rdd):
    """
    From the FL insurance RDD, remove the first line.
    """
    # YOUR CODE HERE
    header = rdd.first()
    return rdd.filter(lambda line: line != header)


# In[31]:


"""
Graded cell

2 points
"""
header = 'policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity'
file = load_file_to_rdd(sc, filePath)
result_rdd = filter_header(sc, file)

assert isinstance(result_rdd, RDD)
assert file.filter(lambda line: line==header).collect()
assert not result_rdd.filter(lambda line: line == header).collect()


# In the following questions, we will work on the file without it's header, it will be stored inside the `file_rdd` variable. You can reuse this variable in your tests.
# 
# ## Question
# 
# Let's try some statistics on the `county` variable, which is the second column of the dataset.

# In[32]:


file_rdd = filter_header(sc, load_file_to_rdd(sc, filePath))


# In[33]:


file_rdd.map(lambda row : row.split(",")).map(lambda row : (row[2],1)).reduceByKey(lambda x,y: x+y).collect()


# In[34]:


def get_county(sc, rdd):
    """
    From the FL insurance RDD, return a RDD containing all of the county.
    We assume the csv is correctly formatted and every line has the correct number of elements.
    """
    # YOUR CODE HERE
    return rdd.map(lambda row : row.split(",")).map(lambda row : (row[2],1))

def county_count(sc, rdd):
    """
    Return a RDD of key,value with county as key, count as values
    """
    # YOUR CODE HERE
    return rdd.reduceByKey(lambda x,y: x+y)


# In[35]:


"""
Graded cell

4 points
"""

# CAREFUL: some tests are invisible so don't try to output a dictionary with what looks like the correct answers :)
file_rdd = filter_header(sc, load_file_to_rdd(sc, filePath))
county_rdd = get_county(sc, file_rdd)

result = dict(county_count(sc, county_rdd).collect())
assert result.get('CLAY COUNTY') == 346


# # Postrequisites

# In[36]:


sc.stop()

