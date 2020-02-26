## 电商推荐系统
* #### 数据加载模块 data-loader
    读取 商品数据文件和用户评分数据文件 解析后初始化到MongoDB.直接用spark DataFrame的write方法写入到MongoDB

* #### 数据初始化模块 statistics-recommender
    根据基础数据 Product和Rating表的数据.使用SparkSql 进行简单的统计转化方便后续环节使用,
    使用sparkSession.read 方法直接读取MongoDB的Rating表数据,作为SparkSQL临时表ratings
    评分最多的商品
      
        select productId,count(productId) as count from ratings group by productId order by count desc
        
    近期评分最多的商品 定义一个UDF函数 对timestamp进行转换成 yyyyMM格式.然后根据productId date进行分组统计
       
        select productId,count(1) as count,yearmonth from ratingOfMonth group by yearmonth,productId order by yearmonth desc,count desc
    
    商品的平均评分
    
        select productId,avg(score) as avg from ratings group by productId order by avg desc
    将统计出的数据写入到MongoDB.

* #### 离线推荐模块 offline-recommender
    综合用户所有的历史数据,利用设定的离线统计算法和离线推荐算法周期性的进行结果统计与保存.主要分为统计推荐,基于隐语义模型的协同过滤
以及基于内容和基于Item-CF的相似推荐.
    项目采用ALS作为协同过滤算法.根据MongoDB中用户评分表计算离线的用户推荐列表和商品相似度矩阵.
    

* #### 在线实时推荐模块 online-recommender
    当用户u 对商品p 进行了评分，将触发一次对u 的推荐结果的更新。由于用户u 对商品p 评分，对于用户u 来说，
他与p 最相似的商品们之间的推荐强度将发生变化，所以选取与商品p 最相似的K 个商品作为候选商品。

* #### 日志数据清洗模块 kafka-stream
    通过flume采集到的日志,由于格式不规范不能直接被spark所处理.使用kafka-stream做一层数据清洗.将清洗后的数据放到 spark订阅的topic,就可以直接消费了

* #### 业务模块 business-server
    整合各个功能 数据流转如下
        
        1.用户通过页面对商品进行评分.
        2.通过埋点日志记录用户评分的日志数据. 前缀:userId|productId|score|timestamp
        3.使用flume采集日志,根据埋点日志的 固定前缀使用正则匹配. 把匹配的到的数据发送到 kafka的 log topic中
        4.kafka-stream 订阅 log topic. 把数据处理成 userId|productId|score|timestamp 这种格式.然后放到recommender topic中.
        5.实时推荐模块订阅recommender模块.当获取到用户评分数据 触发一次实时推荐计算.把更新后的推荐列表写入到MongoDB中
        6.业务系统从MongoDB中 获取实时推荐列表返回给客户展示.
        
        
       
         