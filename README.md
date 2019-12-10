## 1) Discuss the purpose of this database in the context of the startup Sparkify and their analytical goals.

The user base and song database did grow signifcantly so the data warehouse should be moved to a data lake. Hereby, the datasource resides in a S3 bucket containing the origin of the data stored as json logs on user activity as well as songs played in the sparkify app. 

The decision to move forward with a data lake is based on following:
* The team expect that the amount of data will grow continue to exponentially in the future
* In addition the amount of unstructured data will raise as well
* More complex analytical applications will run on the data allowing to discover new insights. This applications require more flexibility in terms of data modelling. An increased flexibility will also allow new roles (e.g. data scientists) to generate insights from data more easily. 

## 2) State and justify your database schema design and ETL pipeline.

The ETL pipeline runs on AWS using S3 Bucket as datasource. Hereby, the data processing is done in AWS EMR using spark using a "Schema-On-Read" approach. Hereby, it is possible to execute data analysis without inserting a predifined schema for tables allowing queries without creating a table specifically. Therefore, also no HDFS e.g. using Hadoop was added into the EMR layer. Instead the results are writen back to the S3 bucket as parquet files.

The etl.py script can be run on a spark node on EMR adding spark submit. The code was tested using etl.ipynb in an local environment reading/writing json data from/to udacity repository.

Overall the approach allows sparkify to run complex processing operations on huge amount of data. But only if its needed. The operations can be executed e.g. once or two times a day making the outcome available for data scientist or business analysts daily. There is no need to run the EMR layer 24 hours resulting in significant cost saving for the startup. 



