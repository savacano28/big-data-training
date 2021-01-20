#!/usr/bin/env python
# coding: utf-8

# Before you turn this problem in, make sure everything runs as expected. First, **restart the kernel** (in the menubar, select Kernel$\rightarrow$Restart) and then **run all cells** (in the menubar, select Cell$\rightarrow$Run All).
# 
# Make sure you fill in any place that says `YOUR CODE HERE` or "YOUR ANSWER HERE", as well as your name and collaborators below:

# In[71]:


NAME = "Stephanya CASANOVA MARROQUIN"
COLLABORATORS = ""


# ---

# For this problem set, we'll be getting used to the Jupyter notebook:
# 
# ![](jupyter.png)
# 
# The Jupyter Notebook is an open-source web application that allows you to create and share documents that contain live code, equations, visualizations and narrative text. We will use it to run Python code.
# 
# The goal of each notebook is to answer a set of questions by implementing code in the specified function. Your implementation should then pass the following locked cell with unit tests.
# 
# To execute a cell, `CTRL + ENTER` when selected. Other shortcuts are available in the `Cell...` tab.

# ## Prerequisites

# In[72]:


# try to edit and execute me :)

print('Hello world from the other side!')


# ---
# ## Warm up
# In this part, we solve very simple Python exercises to warm up.
# 
# ## Question
# 
# Return the sum of two numbers.
# 
# **Hint** : Before each cell with a function you need to implement, we provide you with an empty cell for you to test ideas.

# In[ ]:





# In[73]:


def sum(a, b):    
    return a + b  


# In[74]:


"""
This is an autograded cell. When you execute this cell with your implementation, it should not return any assert errors.
You are being graded by the number of correct unit tests you pass in those cells. 
From my side, we also test your code on private hidden tests so don't try to just solve public ones :).
We also provide you with the score you can get per cell.

1 point
"""
assert sum(1, 2) == 3


# ## Question
# 
# Square all numbers in a list with a list comprehension.
# 
# The following is a list comprehension :
# 
# ```python
# >> numbers = [1, 2, 3]
# >> [x * 2 for x in numbers]
# [2, 4, 6]
# ```

# In[ ]:





# In[75]:


def squared(numbers):
    res = [i**2 for i in numbers] 
    return res


# In[76]:


"""
Graded cell

1 point
"""
assert squared([2, 4, 6]) == [4, 16, 36]


# ## Question
# 
# Implement an algorithm to determine if a list of numbers has all unique numbers, that is each element appears exactly once inside it.

# In[ ]:





# In[77]:


def is_unique(numbers):
    """
    Return True if list of numbers contains only unique numbers, False otherwise.
    """
    # Here, I will help a bit, use a dictionary to store encountered numbers.
    encountered = set()
    for number in numbers:
        if number in encountered:
            return False
        else:
            encountered.add(number)
    return True


# In[78]:


"""
Graded cell

1 point
"""
assert is_unique([2, 5, 9, 7])
assert not is_unique([2, 5, 5, 7])

