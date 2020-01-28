# presentation-spark-101
This repo contains examples on getting started with spark using python.

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
    * docker run -it --rm -p 8888:8888 -p 4040:4040 jupyter/pyspark-notebook
    * For jupyter lab interface use docker run -p 8888:8888 -p 4040:4040 -e JUPYTER_ENABLE_LAB=yes -it --user root -v "$PWD":/home/jovyan/work jupyter/pyspark-notebook

Presentation time is 40 mins

## Into the weeds
1) Spark Context - Setting up a context like what is a context and why do we have to set it up ?
2) Spark Session
3) structured APIs- DataFrame, SQL
4) lowlevel APIs - RDDs, Distributed Variables

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
   * use netcat to run word stream nc -l -p 9999
----
## Next Steps
1) you can check out the spark Graph feature that does cool graph algorithms
2) Writing spark applications 
3) Spark monitoring and tuning
-----
## Resources
1) databricks website
2) Safari Database
3) Spark.Apache.org
4) [pyspark notebooks](https://github.com/jadianes/spark-py-notebooks)
5) [databricks word count 1](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2799933550853697/1880776780418274/2202577924924539/latest.html)
6) [databricks word count 2](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3328674740105987/4033840715400609/6441317451288404/latest.html)
7) [databricks ML example](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8599738367597028/68280419113053/3601578643761083/latest.html)
8) [RDD paper by Matei Zaharia et al](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
9) [Spark computing on clusters by Matei Zaharia](https://www.usenix.org/legacy/event/hotcloud10/tech/full_papers/Zaharia.pdf)
