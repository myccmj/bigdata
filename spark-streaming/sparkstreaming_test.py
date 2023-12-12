import findspark  
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql.functions import from_json, col, udf
import time
from neo4j import GraphDatabase, basic_auth
#import os

# 创建环境
sc = SparkContext(master="local[2]", appName="NetworkWordCount")
ssc = StreamingContext(sc, 10)  # 每隔一秒钟监听一次数据

# 监听端口数据(socket数据源)
lines = ssc.socketTextStream("localhost",9999)




from textblob import TextBlob

# Create a function to get the subjectifvity
# def getSubjectivity(tweet: str) -> float:
#     return TextBlob(tweet).sentiment.subjectivity

# def getSentiment(polarityValue: int) -> str:
#     if polarityValue < 0:
#         return 'Negative'
#     elif polarityValue == 0:
#         return 'Neutral'
#     else:
#         return 'Positive'
    
# Create a function to get the polarity
def getPolarity(tweet: str) -> (str,float):
    return (tweet,TextBlob(tweet).sentiment.polarity)

def write_to_file(rdd):
    if not rdd.isEmpty():
        current_time = time.strftime("%Y%m%d-%H%M%S")
        #pth=os.path.join(r'c:/Users/myccm/Documents/sparkstreaming_output/output',current_time)
        pth=r'c:/Users/myccm/Documents/sparkstreaming_output/'+current_time
        rdd.saveAsTextFile(pth)

neo4j_config = {
    "url": "bolt://localhost:7687",
    "user": "neo4j",
    "password": "12345678"
}
driver = GraphDatabase.driver(neo4j_config["url"], auth=basic_auth(neo4j_config["user"], neo4j_config["password"]))
def write_to_neo4j(rdd):
    if not rdd.isEmpty():
        with driver.session() as session:
            for row in rdd.collect():
                tweet, polarity = row
                session.run("MERGE (t:Tweet {text: $tweet}) SET t.polarity = $polarity", {"tweet": tweet, "polarity": polarity})


polarity = F.udf(getPolarity, FloatType())
#lines.pprint()
polaritys=lines.map(lambda line: getPolarity(line))
polaritys.pprint()
#polaritys.foreachRDD(write_to_file)
polaritys.foreachRDD(write_to_neo4j)
#print(type(polaritys))
#polaritys.saveAsTextFiles(r"c:/Users/myccm/Documents/sparkstreaming_test.txt")

# 开启流式处理
ssc.start()
ssc.awaitTermination()
ssc.stop()
