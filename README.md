# presentation-spark-101
This repo contains examples on getting started with spark. Check out apache spark website for more info. https://spark.apache.org/

## Goals of the presentation:
1. Apache Spark - The idea of this presentation is to give folks who have heard of Spark but not have worked on it a chance to see it in action, give an intro on what can be done and help them start to experiment with it
2. Talk a little theory - This presentation is limited to the working of Spark, we will not go into the Machine Learning aspects of spark just into the data aspects, Spark SQL, some ETL related stuff and Machine Learning
    -- How does the RDD get formed ?
    -- How does dispersion and collection happen ?
    -- What is going on under the hood ?
3. Demo 1 - Counting all the words in a Text file
4. Demo 2 - Wine recommendation example

Presentation time is 40 mins

## Spark Usecases
1. Transformations for ETL
2. Machine Learning
-----
Setting up a context, like what is a context and why do we have to set it up ?
1) Spark Context
2) Spark Session
3) DataFrame 
             i) Manipulations
             ii) SparkSQL

-----
Advantages of using Spark versus Legacy Databases
1) Malform function that takes out stupid stuff in the raw data
2) Where does spark get used in ETL pipelines ?
3) MLLib
-----
## Demo
1) creating RDDs
2) Dataframe Row Operations
3) Streams
-----
## Resources
1) databricks website
