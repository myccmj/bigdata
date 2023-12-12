import findspark
findspark.init()
from pyspark.sql import SparkSession
from neo4j import GraphDatabase, basic_auth
from graphframes import *


from query import *
from neo4j import GraphDatabase
from graphdatascience import GraphDataScience




spark = SparkSession.builder.appName("Neo4jDataLoading").getOrCreate()



# 创建一个到Neo4j数据库的连接
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))

gds = GraphDataScience(driver)


# 连接到Neo4j

#这张图是将作者作为节点，如果他和另一个作者合作过，就增加一条边
#如果多次合作，就有多条边
#因为包中算法是基于边的连接的，而不基于边的属性，因此采用这种方法
node_author=spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://localhost:7687") \
  .option("labels", "author") \
  .load()
edge_coauthor=spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://localhost:7687") \
  .option("query", "MATCH (a)-[r:coauthor]-(b) RETURN a.id AS src , b.id AS dst , r") \
  .load()
graph_author=GraphFrame(node_author,edge_coauthor)

#这张图中是将文章作为节点，如果它和其他文章存在合著者关系，增加一条边
#多条边与上面处理情况相同
node_article=spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://localhost:7687") \
  .option("labels", "article") \
  .load()
edge_sameauthor=spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://localhost:7687") \
  .option("query", "MATCH (a)-[r:sameauthor]-(b) RETURN a.id AS src , b.id AS dst , r") \
  .load()
graph_article=GraphFrame(node_article,edge_sameauthor)

#计算作者图的度
def cal_author_degree():
    return graph_author.inDegrees

#计算文章图的度
def cal_article_degree():
    return graph_article.inDegrees

#寻找某一作者所在强联通组件的 核心作者
#先在作者图上寻找强连通组件，然后在该组件运行PageRank
#name是作者名字，n是返回数目
def find_core_author(name,n):
    # 运行强连通组件算法
    sccResult = graph_author.stronglyConnectedComponents(maxIter=10)

    id = node_author.filter(f'name="{name}"').select('id').collect()[0][0]
    component_id=sccResult.filter(f'id="{id}"').select('component').collect()[0][0]

    # 查找所属的强连通组件
    componentDF = sccResult.filter("component == {}".format(component_id))

    # 在该强连通组件上创建一个新的GraphFrame
    componentGraph = GraphFrame(componentDF, edge_coauthor)

    # 在该强连通组件上运行PageRank算法
    results = componentGraph.pageRank(resetProbability=0.15, maxIter=10)
    sortedResults = results.vertices.sort(results.vertices.pagerank.desc())
    topResults = sortedResults.limit(min(n,sortedResults.count()))
    selected_name=[ row[0] for row in topResults.select('name').collect()]
    return selected_name

#不寻找完整的联通组件，直接在整个图上搞
#先在作者图上寻找强连通组件，然后在该组件运行PageRank
#name是作者名字，n是返回数目
def find_core_author_all(name,n):

    # 运行PageRank算法
    results = graph_author.pageRank(resetProbability=0.15, maxIter=10)
    sortedResults = results.vertices.sort(results.vertices.pagerank.desc())
    topResults = sortedResults.limit(min(n,sortedResults.count()))
    selected_name=[ row[0] for row in topResults.select('name').collect()]
    return selected_name

#寻找某一作者所在强联通组件的 核心作者
#先在作者图上寻找强连通组件，然后在该组件运行PageRank
#name是作者名字，n是返回数目
def find_core_article(name,n):
    # 运行强连通组件算法
    sccResult = graph_article.stronglyConnectedComponents(maxIter=10)

    id = node_article.filter(f'name="{name}"').select('id').collect()[0][0]
    component_id=sccResult.filter(f'id="{id}"').select('component').collect()[0][0]

    # 查找所属的强连通组件
    componentDF = sccResult.filter("component == {}".format(component_id))

    # 在该强连通组件上创建一个新的GraphFrame
    componentGraph = GraphFrame(componentDF, edge_sameauthor)

    # 在该强连通组件上运行PageRank算法
    results = componentGraph.pageRank(resetProbability=0.15, maxIter=10)
    sortedResults = results.vertices.sort(results.vertices.pagerank.desc())
    topResults = sortedResults.limit(min(n,sortedResults.count()))
    selected_name=[ row[0] for row in topResults.select('name').collect()]
    return selected_name

 #寻不寻找强连通组件
#先在作者图上寻找强连通组件，然后在该组件运行PageRank
#name是作者名字，n是返回数目   
def find_core_article_all(name,n):

    # 运行PageRank算法
    results = graph_article.pageRank(resetProbability=0.15, maxIter=10)
    sortedResults = results.vertices.sort(results.vertices.pagerank.desc())
    topResults = sortedResults.limit(min(n,sortedResults.count()))
    selected_name=[ row[0] for row in topResults.select('name').collect()]
    return selected_name


#一些测试
#print(find_core_author("Luo, Jingwen",4))
#print(find_core_article("Development and Evaluation of Deep Learning Models for Automated Estimation of Myelin Maturation Using Pediatric Brain MRI Scans",4))
#print(find_core_author_all("Luo, Jingwen",4))
#print(find_core_article_all("Development and Evaluation of Deep Learning Models for Automated Estimation of Myelin Maturation Using Pediatric Brain MRI Scans",4))

#先执行这个再执行get_similar_author和get_similat_article 执行一次就可

def create_graph_project(driver):
    with driver.session() as session:
        query ='''
    CALL gds.graph.project(
    "TotalGraph",
    ["author", "article","field"],
    ["WRITE","WRITTEN","contain","belong","coauthor","sameauthor"]
)
    YIELD graphName, nodeCount, relationshipCount
'''
        session.run(query)
#依据名字查询相似性最高的作者
#使用neo4j ds


def get_similar_author(driver,name):
    with driver.session() as session:
        query = f'''
CALL gds.nodeSimilarity.filtered.stream("TotalGraph", {{sourceNodeFilter:"author" , targetNodeFilter:"author" }} )
YIELD node1, node2, similarity
WHERE gds.util.asNode(node1).name="{name}"
RETURN gds.util.asNode(node1).name AS author1, gds.util.asNode(node2).name AS author2, similarity
ORDER BY similarity DESCENDING, author1, author2
LIMIT 10
        '''

        result = list(session.run(query))
        #结构是这样的，result是列表，其中元素为字典，三个键，author1,author2,similarity author1为查询作者
        return result




def get_similar_article(driver,name):
    with driver.session() as session:
        query = f'''
CALL gds.nodeSimilarity.filtered.stream("TotalGraph", {{sourceNodeFilter:"article" , targetNodeFilter:"article" }} )
YIELD node1, node2, similarity
WHERE gds.util.asNode(node1).name="{name}"
RETURN gds.util.asNode(node1).name AS article1, gds.util.asNode(node2).name AS article2, similarity
ORDER BY similarity DESCENDING, article1, article2
LIMIT 10
        '''

        result = list(session.run(query))
        #结构是这样的，result是列表，其中元素为字典，三个键，article1,article2,similarity article1为查询文章
        return result

# test
#data = get_similar_author(driver, "Pedrycz, Witold")
#data = get_similar_article(driver,"Development and Evaluation of Deep Learning Models for Automated Estimation of Myelin Maturation Using Pediatric Brain MRI Scans")


def create_author_project(driver):
    with driver.session() as session:
        query ='''
    CALL gds.graph.project(
    "AuthorGraph",
    ["author"],
    ["coauthor"]
)
    YIELD graphName, nodeCount, relationshipCount
'''
        session.run(query)
        
def create_article_project(driver):
    with driver.session() as session:
        query ='''
    CALL gds.graph.project(
    "ArticleGraph",
    ["article"],
    ["sameauthor"]
)
    YIELD graphName, nodeCount, relationshipCount
'''
        session.run(query)


#计算度中心性
#参数为指定是要计算作者图还是文章图
#作者图的名字是AuthorGraph 文章图是ArticleGraph

def degree_cen(driver,graphname):
    with driver.session() as session:
        query = f'''
CALL gds.degree.stream("{graphname}")
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS name, score AS followers
ORDER BY followers DESC, name DESC
        '''

        result = list(session.run(query))
        return result

#中介中心性
def between_cen(driver,graphname):
    with driver.session() as session:
        query = f'''
CALL gds.degree.stream("{graphname}")
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS name, score
ORDER BY name ASC
        '''

        result = list(session.run(query))
        return result


#接近中心性
def close_cen(driver,graphname):
    with driver.session() as session:
        query = f'''
CALL gds.degree.stream("{graphname}")
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS id, score
ORDER BY score DESC
        '''

        result = list(session.run(query))
        return result

#
#create_author_project(driver)

#print(degree_cen(driver,"AuthorGraph"))
