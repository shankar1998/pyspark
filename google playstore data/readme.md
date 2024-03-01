# Procedure

## 1.Create the cluster 

## 2.load the csv file in the databricks and note the path

## 3.create a notebook

## 4.import necessary functions

import pyspark
from pyspark.sql import *
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql.functions import *


## 5.create a DF and load the csv file 

df=spark.read.load('/FileStore/tables/googlestore.csv',format='csv',header='true',sep=',',escape='"',inferschema='true')

## 6.check and display the dataframe

df.show()
df.count()
df.printSchema()

## 7.Data cleaning

#drop unwanted columns
df=df.drop("Size","Content Rating","Last Updated","Android Ver")

#Changing to correct datatypes
df = df.withColumn('Installs', df['Installs'].cast(IntegerType()))

df = df.withColumn('Reviews', df['Reviews'].cast(IntegerType()))

df = df.withColumn("Price", df["Price"].cast(IntegerType()))

#removing extra symbols 
df=df.withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))

df=df.withColumn("Price",regexp_replace(col("Price"),"[$]",""))

## 8. Create a temp view to query on the data

df.createOrReplaceTempView("Apps")

## 9.Quering on data

%sql
select app,sum(reviews) from apps group by app order by sum(reviews) desc limit 10

%sql
select app,type,sum(installs) from apps group by app,type order by 3 desc 

%sql

select app, sum(price) from apps where type='Paid' group by app order by sum(price)desc


