#!/usr/bin/env python
# coding: utf-8

# Before you turn this problem in, make sure everything runs as expected. First, **restart the kernel** (in the menubar, select Kernel$\rightarrow$Restart) and then **run all cells** (in the menubar, select Cell$\rightarrow$Run All).
# 
# Make sure you fill in any place that says `YOUR CODE HERE` or "YOUR ANSWER HERE", as well as your name and collaborators below:

# In[1]:


NAME = "Stephanya CASANOVA MARROQUIN"
COLLABORATORS = ""


# ---

# # SparkSQL
# 
# In this notebook, we introduce SparkSQL, Spark's interface for working with structured data. From Spark 2.0 and forward, this is the preferred way of implementing Spark code, as it contains all of the latest optimisations.
# 
# PySpark benefits a lot from SparkSQL, as there is performance parity between Scala, Java, Python and R interfaces for Spark which use the same optimizer. 

# ---
# ## Prerequisites
# 
# Before running Spark code, we need to start a SparkSession instance. The following block will be common to every notebook so you can run your code.
# 
# While your SparkSession is running, you can hit `http://localhost:4040` to get an overview of your Spark local cluster and all operations ongoing.

# In[2]:


from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('lecture-lyon2').setMaster('local')
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark


# In[3]:


# Import other important libraries

from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql.types import *

import numpy as np
import pandas as pd
from bokeh.plotting import figure, show, ColumnDataSource
from bokeh.models import HoverTool
from bokeh.io import output_notebook

output_notebook()


# ---
# ## Part A - On to DataFrames / Datasets
# 
# A `Dataset` is a distributed collection of data which provides the benefits of RDDs (strong typing, ability to use lambda functions) with the benefits of SparkSQL's optimized execution engine.
# 
# A `DataFrame` is a `Dataset` organized into named columns. It is conceptually equivalent to a table in a relational database, or a data frame in Python/R. Conceptually, a `DataFrame` is a `Dataset` of `Row`s.
# 
# As with RDDs, applications can create DataFrames from an existing RDD, a Hive table or from Spark data sources.

# # Question
# 
# Recall from the previous assignment how we used two tables on students : one for students to grades, another one for students to gender. Let's create a function which takes a RDD of Row and a schema as arguments and generates the corresponding DataFrame.

# In[ ]:





# In[4]:


def create_dataframe(spark, rdd, schema):
    """
    Generate a DataFrame from a RDD of Rows and a schema.
    We assume the RDD is correctly formatted, no need to check for anything.
    """
    return spark.createDataFrame(rdd, schema)


# In[5]:


"""
Graded cell

2 points
"""
rdd = spark.sparkContext.parallelize([('1', 'a'), ('2', 'b'), ('3', 'c'), ('4', 'd'), ('5', 'e'), ('6', 'f')])
schema = StructType([StructField('ID', StringType(), True), StructField('letter', StringType(), True)])

result_df = create_dataframe(spark, rdd, schema)
assert result_df.schema == schema
assert result_df.rdd.collect() == rdd.collect()


# Let's generate a Dataframe of the students tables for the incoming questions, using our newly created `create_dataframe` function. We also create temporary views for those DataFrames so we can interact with them in SQL.

# In[6]:


genders_rdd = spark.sparkContext.parallelize([('1', 'M'), ('2', 'M'), ('3', 'F'), ('4', 'F'), ('5', 'F'), ('6', 'M')])
grades_rdd = spark.sparkContext.parallelize([('1', 5), ('2', 12), ('3', 7), ('4', 18), ('5', 9), ('6', 5)])

genders_schema = StructType([StructField('ID', StringType(), True), StructField('gender', StringType(), True)])
grades_schema = StructType([StructField('ID', StringType(), True), StructField('grade', StringType(), True)])

genders_df = create_dataframe(spark, genders_rdd, genders_schema)
grades_df = create_dataframe(spark, grades_rdd, grades_schema)

genders_df.createOrReplaceTempView('genders')
grades_df.createOrReplaceTempView('grades')


# You have two ways of interacting with a Dataframe :
# 
# * DataFrames provide a domain-specific language for structured manipulation :
# 
# ```python
# >> genders_df.filter(genders_df['ID'] > 2)
# +---+------+
# | ID|gender|
# +---+------+
# |  3|     F|
# |  4|     F|
# |  5|     F|
# |  6|     M|
# +---+------+
# ```
# 
# In the more simple cases, you can interact with DataFrames with a syntax close to the Pandas syntax.
# 
# ```python
# >> genders_df[genders_df['ID'] > 2]
# +---+------+
# | ID|gender|
# +---+------+
# |  3|     F|
# |  4|     F|
# |  5|     F|
# |  6|     M|
# +---+------+
# ```
# 
# * The `sql` function of a SparkSession enables to run SQL queries directly on the frame and returns a DataFrame, on which you can continue your computations
# 
# ```python
# # Register the DataFrame as a SQL temporary view beforehand
# >> genders_df.createOrReplaceTempView('genders')
# >> spark.sql('SELECT * FROM genders WHERE ID > 2').show()
# +---+------+
# | ID|gender|
# +---+------+
# |  3|     F|
# |  4|     F|
# |  5|     F|
# |  6|     M|
# +---+------+
# ```
# 
# Don't hesitate to check the [DataFrame Function Reference](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions) for all of the operators you can use on a DataFrame. Use the following cell to experiment :)

# In[7]:


# Use this cell to practice your new SQL skills
genders_df[genders_df['ID'] > 4].show()


# # Question
# 
# Remember the mean grade per gender question from last assignment ? Remember how unpleasant it was ? Let's do that directly in SparkSQL ! You can do it with whatever way pleases you between programmatic SQL or SparkSQL DSL. 
# 
# PS : if you are using programmatic SQL interaction, you may want to define a temporary view of temporary variables. You may want to delete those views at the end of your function with `spark.catalog.dropTempView('your_view')`. Be careful if removing the view, DataFrame are also lazily computed so don't delete your view if you still have not computed and cached the resulting DataFrame.

# In[8]:


genders_df.createOrReplaceTempView('genders')
grades_df.createOrReplaceTempView('grades')
result_df=spark.sql('SELECT gender, AVG(grade) as grade FROM genders inner join grades on genders.ID= grades.ID group by gender').toPandas()
#result_df.columns== ['gender', 'grade']
result_df[result_df['gender'] == 'F'].values[0][1] - 11.3 < 0.1
result_df[result_df['gender'] == 'M'].values[0][1] - 7.3 < 0.1


# In[9]:


def mean_grade_per_gender(spark, genders_df, grades_df):
    """
    Given a RDD of studentID to grades and studentID to gender, compute mean grade for each gender returned as paired RDD.
    Assume all studentIDs are present in both RDDs, making inner join possible, no need to check that.
    Schema of output dataframe should bee gender, mean.
    """
    # YOUR CODE HERE
    genders_df.createOrReplaceTempView('genders')
    grades_df.createOrReplaceTempView('grades')
    return spark.sql('SELECT gender, AVG(grade) as grade FROM genders inner join grades on genders.ID= grades.ID group by gender')


# In[10]:


"""
Graded cell

3 points
"""
result_df = mean_grade_per_gender(spark, genders_df, grades_df).toPandas()
result_df.columns == ['gender', 'grade']

assert result_df[result_df['gender'] == 'F'].values[0][1] - 11.3 < 0.1
assert result_df[result_df['gender'] == 'M'].values[0][1] - 7.3 < 0.1


# ---
# ## Part B - Descriptive statistics in SparkSQL
# 
# Let's reload the `FL_insurance_sample.csv` file from last assignment and freely interact with it.
# 
# # Question
# 
# Load the file by giving a path to the file

# In[ ]:





# In[11]:


def read_csv(spark, path):
    """
    Create a DataFrame by loading an external csv file. We don't expect any formatting nor processing here. 
    We assume the file has a header, uses " as double quote and , as delimiter. Infer its schema automatically.
    You don't need to raise an exception if the file does not exist.    
    """
    # YOUR CODE HERE
    spark.createDataFrame(rdd, schema)


# In[12]:


"""
Graded cell

2 points
"""
file_path = 'FL_insurance_sample.csv'
result_df = read_csv(spark, file_path)

assert result_df.schema == StructType([
    StructField('policyID',IntegerType(),True),
    StructField('statecode',StringType(),True),
    StructField('county',StringType(),True),
    StructField('eq_site_limit',DoubleType(),True),
    StructField('hu_site_limit',DoubleType(),True),
    StructField('fl_site_limit',DoubleType(),True),
    StructField('fr_site_limit',DoubleType(),True),
    StructField('tiv_2011',DoubleType(),True),
    StructField('tiv_2012',DoubleType(),True),
    StructField('eq_site_deductible',DoubleType(),True),
    StructField('hu_site_deductible',DoubleType(),True),
    StructField('fl_site_deductible',DoubleType(),True),
    StructField('fr_site_deductible',IntegerType(),True),
    StructField('point_latitude',DoubleType(),True),
    StructField('point_longitude',DoubleType(),True),
    StructField('line',StringType(),True),
    StructField('construction',StringType(),True),
    StructField('point_granularity',IntegerType(),True)
])


# # Question
# 
# Let's plot the number of different counties in a histogram, like in the previous assignment. We have imported the `bokeh` module for interactive plotting. To do that, return a Pandas a dataframe which contains, for each county, the number of its occurences in the dataset.
# 
# _Hint: a Spark Dataframe is distributed on a number of workers, so it cannot be plotted as is. You will need to collect the data you want to plot back in the driver. The `toPandas` is usable to retrieve a Pandas local Dataframe, be careful to only use it on small Dataframes !_

# In[ ]:


insurance_df = read_csv(spark, 'FL_insurance_sample.csv')
insurance_df.createOrReplaceTempView('insurance')


# In[ ]:





# In[ ]:


def count_county(spark, insurance_df):
    """
    Return a Pandas a dataframe which contains, for each county, the number of its occurences in the dataset. 
    Schema of the Dataframe should be ['county', 'count']    
    """
    # YOUR CODE HERE
    raise NotImplementedError()


# In[ ]:


"""
Graded cell

3 points
"""
df = count_county(spark, insurance_df)
result = df.set_index('county').to_dict()['count']

assert result.get('CLAY COUNTY') == 346


# In[ ]:


# Plot it for fun with bokeh.
data = count_county(spark, insurance_df)

source = ColumnDataSource(data)

hover = HoverTool(tooltips=[
    ("type", "@county"),
    ("count", "@count"),
])

p = figure(x_range=data['county'].values, plot_height=250, title="County counts", tools=[hover])

p.vbar(x='county', top='count', width=0.9, source=source)

p.xgrid.grid_line_color = None
p.y_range.start = 0

show(p)


# # Postrequisites

# In[ ]:


spark.stop()

