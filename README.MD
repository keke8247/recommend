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

* #### 在线实时推荐模块 online-recommender

* #### 业务模块 business-server