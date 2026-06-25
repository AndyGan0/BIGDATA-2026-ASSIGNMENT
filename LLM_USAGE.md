#  <b> LLM Usage </b>

LLM models have been used as a tool in this assignment. 

The initial structure of the source code was based on the code of the lab examples. The logic behind the source code is NOT based on LLM models. However, LLM models were used in order to find the correct syntax of certain commands and to find the appropriate commands in order to execute certain tasks. 

Moreover, LLM models were used at the end to find bugs in the code and verify that the code is correct, as well as assess the results and better understand the whole assignment.

In some sections, multiple LLM models were used and their answers were compared in order to verify that they are correct.

Below are some examples of sections that LLM helped in this assignment:


## Query 1

* LLMs were used to find which parts are unecessary for this assignment in the original lab examples. The code was simplified.

* LLMs helped read the two csv files together as one dataframe.

* LLMs helped calculate the percentage using a window over the whole dataframe.

* LLMs suggested to cache the dataframe so that it's not calculated twice.


## Query 2

* LLMs were used in both dataframe and sql versions to eliminate months that didn't make it to the top 3 in each year. This was achieved by calculating the ranking of each row through a window that partitions by the year.



## Query 3

* In dataframe version, LLM helped find the source of a bug. Rows with no reported income were kept in the dataframe with None values and were printed in the end result.

* In RDD version, LLMs found the source of a bug that caused error during execution. Non-numeric incomes were being casted to floats. 



## Query 4

* LLMs helped structure the command for calculating the haversine distance. However, the formula was verified as well to make sure it's correct.

* LLMs helped to keep for each crime only the row that contains the minimum distance, thus keeping the row with the closest station. This was achieved by finding the ranking through a window and then filtering.


