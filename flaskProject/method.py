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

def find_core_author_all(name,n):
    print("use pagerank to find relevant author")
    selected_name="abc"
    #运行PageRank算法
    results = graph_author.pageRank(resetProbability=0.15, maxIter=6)
    sortedResults = results.vertices.sort(results.vertices.pagerank.desc())
    topResults = sortedResults.limit(min(n,sortedResults.count()))
    selected_name=[ row[0] for row in topResults.select('name').collect()]
    return selected_name

def get_similar_author(driver,name):
    with driver.session() as session:
        query = f'''
CALL gds.nodeSimilarity.filtered.stream("TotalGraph", {{sourceNodeFilter:"author" , targetNodeFilter:"author" }} )
YIELD node1, node2, similarity
WHERE gds.util.asNode(node1).name="{name}"
RETURN gds.util.asNode(node1).name AS author1, gds.util.asNode(node2).name AS author2,gds.util.asNode(node2).id AS a2id, similarity
ORDER BY similarity DESCENDING, author1, author2
LIMIT 10
        '''

        result = list(session.run(query))
        #结构是这样的，result是列表，其中元素为字典，三个键，author1,author2,similarity author1为查询作者
        return result

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
