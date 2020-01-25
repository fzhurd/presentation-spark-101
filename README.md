# presentation-spark-101
This repo contains examples on getting started with spark. Check out [apache spark](https://spark.apache.org/) website for more info 

## Places to get Containerized Spark
* [gettyimages](https://github.com/gettyimages/docker-spark)
* [PySpark Notebook](https://github.com/jupyter/docker-stacks/tree/master/pyspark-notebook)

## Goals of the presentation:
1. Apache Spark - The idea of this presentation is to give folks who have heard of Spark but not have worked on it a chance to see it in action. Give an intro on what its about and how they can get started.
2. Talk a little theory - This presentation is limited to the working of Spark, we will not go into the Machine Learning aspects of spark just into the data aspects, Spark SQL, some ETL related stuff and Machine Learning
    * How does the RDD get formed ?
    * How does dispersion and collection happen ?
    * What is going on under the hood ?
    * Actions, transformations and Numerics
    * Useful command list- 
                    -- sc.parallelize()
                    -- sc.range()
                    -- sc.collect() 
    * [Get the PySpark - Jupyter Docker image](https://hub.docker.com/r/jupyter/pyspark-notebook)
    * docker run -it --rm -p 8888:8888 jupyter/pyspark-notebook
                    
3. Demo 1 - Counting all the words in a Text file
4. Demo 2 - ML example

Presentation time is 40 mins

## Into the weeds
1) Spark Context - Setting up a context like what is a context and why do we have to set it up ?
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
1) Dataframes
2) SparkSQL
3) RDDs
4) MLlib
5) Streams
----
## Next Steps
1) you can check out the spark Graph feature that does cool graph algorithms
2) Writing spark applications 
3) Spark monitoring and tuning
-----
## Resources
1) databricks website
2) Safari Database

