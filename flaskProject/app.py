from flask import Flask,render_template,request,flash,redirect,url_for
from flask_wtf import FlaskForm
from wtforms import StringField,PasswordField,SelectField,SearchField,SubmitField,TextAreaField
from wtforms.validators import DataRequired
# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
from neo4j import GraphDatabase, basic_auth
#from graphframes import *
from graphdatascience import GraphDataScience
from method import *
import socket

socket_flag=False
print("init....")
authors = []
author=''
article=''
relevant_authors=[]
articles=[]
reviews=[]
# socket相关，为了与sparkstreaming连接
host = 'localhost'
port = 9999
if(socket_flag):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(1)
    print('\nListening for a client at', host, port)
    # 等待连接
    conn, addr = s.accept()
    print('\nConnected by', addr)
#////////////////////////////////////////

# 创建驱动程序实例
URI="neo4j://neo4j:7687"
print("URI",URI)
driver=GraphDatabase.driver(URI, auth=("neo4j", "12345678"))
driver.verify_connectivity()
gds = GraphDataScience(driver)
# spark = SparkSession.builder.appName("Neo4jDataLoading").getOrCreate()
# #这张图是将作者作为节点，如果他和另一个作者合作过，就增加一条边
# #如果多次合作，就有多条边
# #因为包中算法是基于边的连接的，而不基于边的属性，因此采用这种方法
# node_author=spark.read.format("org.neo4j.spark.DataSource") \
#   .option("url", "bolt://localhost:7687").option("authentication.basic.username", "neo4j").option("authentication.basic.password", "12345678") \
#   .option("labels", "author") \
#   .load()
# edge_coauthor=spark.read.format("org.neo4j.spark.DataSource") \
#   .option("url", "bolt://localhost:7687").option("authentication.basic.username", "neo4j").option("authentication.basic.password", "12345678") \
#   .option("query", "MATCH (a)-[r:coauthor]-(b) RETURN a.id AS src , b.id AS dst , r") \
#   .load()
# graph_author=GraphFrame(node_author,edge_coauthor)
# author_pagerank_results = graph_author.pageRank(resetProbability=0.15, maxIter=6)
create_graph_project(driver)
print("init finish")


app = Flask(__name__)
app.secret_key="abc"

class LoginForm(FlaskForm):
    user=StringField("用户名")
    pwd=PasswordField("密码")
    search=SearchField("搜索作者")
    search1 = SearchField("搜索文章")
    submit=SubmitField("提交")

class ReviewForm(FlaskForm):
    review = TextAreaField("有什么想对我们说嘛")
    submit = SubmitField("发送")

class SelectForm(FlaskForm):
    my_choices = [('1', 'pagerank推荐'), ('2', 'similarNode推荐')]
    my_field = SelectField('My Field', choices=my_choices)
    submit = SubmitField("确认")



def searchAuthor(name):
    namelist=name.split()
    name='.*'
    for x in namelist:
        name+=x+'.*'
    records, summary, keys = driver.execute_query(
    "MATCH (n:author) where toLower(n.name) =~ toLower($name) RETURN n limit 15",
    name=name,
    database_="neo4j",
    )
    print(records)
    print(summary)
    print(keys)
    return records

def searchArticle(name):
    namelist=name.split()
    name='.*'
    for x in namelist:
        name+=x+'.*'
    records, summary, keys = driver.execute_query(
    "MATCH (n:article) where toLower(n.name) =~ toLower($name) RETURN n limit 15",
    name=name,
    database_="neo4j",
    )
    print(records)
    print(summary)
    print(keys)
    return records

def searchReview():
    records, summary, keys = driver.execute_query(
    "MATCH (t:Tweet) RETURN t",
    database_="neo4j",
    )
    print(records)
    print(summary)
    print(keys)
    return records

def searchArticlefromAuthor(name):
    global driver
    records, summary, keys = driver.execute_query(
    "MATCH (n:author)-[r]->(a:article) where n.name =$name RETURN a",
    name=name,
    database_="neo4j",
    )
    print(records)
    print(summary)
    print(keys)
    return records

def searchAuthorfromArticle(name):
    records, summary, keys = driver.execute_query(
        "MATCH (n:author)-[r]->(a:article) where a.name =$name RETURN n",
        name=name,
        database_="neo4j",
    )
    print(records)
    print(summary)
    print(keys)
    return records

def searchFieldfromArticle(name):
    records, summary, keys = driver.execute_query(
        "MATCH (n:field)-[r]->(a:article) where a.name =$name RETURN n",
        name=name,
        database_="neo4j",
    )
    print(records)
    print(summary)
    print(keys)
    return records


@app.route('/wtform',methods=['GET','POST'])
def wtf():
    global authors
    global articles
    global conn
    global reviews
    fr=LoginForm()
    rfr = ReviewForm()
    if (request.method == 'POST'):
        if(rfr.validate_on_submit()):
            review=rfr.review.data
            print(review)
            #发送消息到sparkstreaming
            s_spark=review+'\n'
            if(socket_flag):
                conn.send(s_spark.encode('utf-8'))
                reviews=[]
        else:
            user = request.form.get('user')
            pwd = request.form.get('pwd')
            name = request.form.get('search')
            name1=request.form.get('search1')
            print(user,pwd,name,name1)
            flash(user)
            flash(pwd)
            flash(name)
            authors=[]
            articles = []
            if(name):

                result=searchAuthor(name)
                for x in result:
                    authors.append(x.data()['n'])
            if (name1):

                result = searchArticle(name1)
                for x in result:
                    articles.append(x.data()['n'])
    if (not len(reviews)):
        neoRvs = searchReview()
        for rv in neoRvs:
            tmp = rv.data()
            reviews.append(tmp['t'])

    return render_template('index.html',form=fr,wtf='true',authors=authors,articles=articles,reviewform=rfr,reviews=reviews)

@app.route('/',methods=['GET','POST'])
def hello_world():  # put application's code here
    print("hello world")
    global authors
    global articles
    global conn
    global reviews
    fr = LoginForm()
    rfr = ReviewForm()
    if (request.method == 'POST'):
        if (rfr.validate_on_submit()):
            review = rfr.review.data
            print(review)
            # 发送消息到sparkstreaming
            s_spark = review + '\n'
            if (socket_flag):
                conn.send(s_spark.encode('utf-8'))
                reviews = []
        else:
            user = request.form.get('user')
            pwd = request.form.get('pwd')
            name = request.form.get('search')
            name1 = request.form.get('search1')
            print(user, pwd, name, name1)
            flash(user)
            flash(pwd)
            flash(name)
            authors = []
            articles = []
            if (name):

                result = searchAuthor(name)
                for x in result:
                    authors.append(x.data()['n'])
            if (name1):

                result = searchArticle(name1)
                for x in result:
                    articles.append(x.data()['n'])
    if (not len(reviews)):
        neoRvs = searchReview()
        for rv in neoRvs:
            tmp = rv.data()
            reviews.append(tmp['t'])

    return render_template('index.html', form=fr, wtf='true', authors=authors, articles=articles, reviewform=rfr,
                           reviews=reviews)

@app.route('/article',methods=['GET','POST'])
def article():  # put application's code here
    global authors
    global driver
    global article
    print("article",article)
    r = searchAuthorfromArticle(article['name'])
    r1 = searchFieldfromArticle(article['name'])
    authors=[]
    field=[]
    for x in r:
        authors.append(x.data()['n'])
    for x in r1:
        field.append(x.data()['n'])
    return render_template('article.html',authors=authors,article=article,field=field)

@app.route('/author',methods=['get', 'post'])
def author():
    # 在这里编写你的Python函数逻辑
    global author
    global driver
    global articles
    global relevant_authors
    print("author",author)
    r=searchArticlefromAuthor(author['name'])
    articles=[]
    fr = SelectForm()
    for x in r:
        articles.append(x.data()['a'])
    if (request.method == 'POST'):
        if(fr.validate_on_submit()):
            relevant_authors = []
            ch=fr.my_field.data
            print(ch)
            if(int(ch)==1):
                print("pagerank")
                #r=find_core_author_all(graph_author,author['name'],10)
                data = get_similar_author(driver, author['name'])
                for x in data:
                    tmp = {'name': x['author2'], 'id': x['a2id']}
                    relevant_authors.append({'name': x['author2'], 'similarity': x['similarity'], 'id': x['a2id']})
                    if (tmp not in authors):
                        authors.append(tmp)
            else:
                print("similarnode")
                data = get_similar_author(driver, author['name'])
                for x in data:
                    tmp={'name':x['author2'],'id':x['a2id']}
                    relevant_authors.append({'name':x['author2'],'similarity':x['similarity'],'id':x['a2id']})
                    if(tmp not in authors):
                        authors.append(tmp)

        else:
            print("fault")
    return render_template('author.html',author=author,articles=articles,form=fr,relevant_authors=relevant_authors)



@app.route('/gotoauthor/<author_id>')
def gotoauthor(author_id):
    # 在这里编写你的Python函数逻辑
    global authors
    global author
    global  relevant_authors
    author=''
    print(author_id)
    print(authors)
    for x in authors:
        if(int(x['id'])==int(author_id)):
            author=x
            print(author)
            break
    relevant_authors=[]
    return redirect('/author')
    #return render_template('author.html',author=author)

@app.route('/gotoarticle/<article_id>')
def gotoarticle(article_id):
    # 在这里编写你的Python函数逻辑
    global authors
    global article
    global articles
    print(article_id)
    print(authors)
    print(articles)
    for x in articles:
        if(x['id']==int(article_id)):
            article=x
            print(x)
            break
    authors=[]
    return redirect('/article')

@app.route('/button')
def python_function():
    # 在这里编写你的Python函数逻辑
    print("call")
    return render_template('button.html')




if __name__ == '__main__':
    app.run(port=5050)
