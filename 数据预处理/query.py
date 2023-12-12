# %%
import findspark
findspark.init()
from pyspark.sql import SparkSession
from neo4j import GraphDatabase, basic_auth
from graphframes import *

# 这些函数返回的是spark中的dataframe


###获取一个作者的所有文章
def get_article_by_author(spark,name):
    return spark.read.format("org.neo4j.spark.DataSource") \
    .option("url", "bolt://localhost:7687") \
    .option("query", f'MATCH (a)-[r:WRITE]-(b) WHERE a.name="{name}" RETURN b.name') \
    .load()


### 获取一个文章的所有作者
def get_author_by_a(spark,name):
    return spark.read.format("org.neo4j.spark.DataSource") \
    .option("url", "bolt://localhost:7687") \
    .option("query", f'MATCH (a)-[r:WRITTEN]-(b) WHERE b.name="{name}" RETURN a.name') \
    .load()

  
        


