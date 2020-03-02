## 电商推荐系统
  本项目参考 尚硅谷大数据电商推荐系统.
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

* #### 基于内容的离线推荐模块
    在商品信息tag属性中,有用于给商品打的标签.通过对标签内容进行提取,得到商品的内容特征向量,进而求取相似度矩阵,为了避免热门标签对特征提取的影响,
    采用TF-IDF算法对标签的权重进行调整,从而尽可能的贴近用户的偏好.
    
* #### 基于物品的协同过滤相似推荐
    Item-CF,通过收集用户的行为信息(点击,评分,分享,收藏,购买......)得到商品间的相似度.
    如果两个商品有同样的受众,那么认为这两个商品有内在相关性.利用已有采集到的用户行为数据,分析商品受众的相似程度.进而分析出商品的相似程度.
    这种方法定义为物品的 "同现相似度"
    公式如下 : |Ni ∩ Nj| /根号(Ni*Nj) 
    Ni 表示对商品i 的行为用户数据数量
    Nj 表示对商品j 的用户行为数据数量
     

* #### 业务模块 business-server
    整合各个功能 数据流转如下
        
        1.用户通过页面对商品进行评分.
        2.通过埋点日志记录用户评分的日志数据. 前缀:userId|productId|score|timestamp
        3.使用flume采集日志,根据埋点日志的 固定前缀使用正则匹配. 把匹配的到的数据发送到 kafka的 log topic中
        4.kafka-stream 订阅 log topic. 把数据处理成 userId|productId|score|timestamp 这种格式.然后放到recommender topic中.
        5.实时推荐模块订阅recommender模块.当获取到用户评分数据 触发一次实时推荐计算.把更新后的推荐列表写入到MongoDB中
        6.业务系统从MongoDB中 获取实时推荐列表返回给客户展示.
        
        
       
         