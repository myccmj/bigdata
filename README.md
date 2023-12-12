# bigdata
云计算与大数据平台2023
构建了一个科研合作平台，利用了sparkstreaming、graphframes、neo4j、flask等工具实现<br>
部署前需下载neo4j的gds插件neo4j-graph-data-science-2.5.5.jar放至flaskProject/neo4j/plugins下<br>
进入flaskProject通过docker进行部署，若成功部署，则可进入localhost:5050进行查看（flaskapp可能一开始无法正常启动，因为要等待neo4j先启动，需要过一会再启动flaskapp)<br>


