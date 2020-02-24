package com.wdk.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jblas.DoubleMatrix

/**
  * @Description
  *             离线推荐 基于隐语义模型的协同过滤算法
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/2/23 17:51
  * @Since version 1.0.0
  */
object OfflineRecommender {
    //定义 mongo Collection
    val MONGO_RATING_COLLECTION = "Rating"

    val USER_RECS = "UserRecs"  //用户推荐列表

    val PRODUCT_RECS = "ProductRecs" //商品相似列表


    val TOP_20 = 20

    def main(args: Array[String]): Unit = {
        //定义配置map
        val config = Map(
            "spark.cores"->"local[1]",  //这里要一个核去跑  不然 group by 之后再order by 得到数据局部有序
            "mongo.uri" -> "mongodb://localhost:27017/recommend",
            "mongo.db" -> "recommend"
        )

        //创建sparkConf
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender");

        //创建SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate();

        import spark.implicits._
        //读取数据
        val ratingRDD = spark.read
            .option("uri",config("mongo.uri"))
            .option("collection",MONGO_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[ProductRating]
            .rdd
            .map(rating => {
                (rating.userId,rating.productId,rating.score)
            }).cache()

        //取出所有用户和商品集
        val users = ratingRDD.map(_._1).distinct()
        val products = ratingRDD.map(_._2).distinct()

        // 核心计算过程
        // 1. 训练隐语义模型
        val trainData = ratingRDD.map(item => Rating(item._1,item._2,item._3)) //把RDD转换为训练模型需要的数据集
        // 定义模型训练的参数，rank隐特征个数，iterations迭代词数，lambda正则化系数
        val (rank,iterations,lambda) = (5,10,0.1)
        val model = ALS.train(trainData,rank,iterations,lambda); //得到模型

        // 2. 获得预测评分矩阵，得到用户的推荐列表
        // 用userRDD和productRDD做一个笛卡尔积，得到空的userProductsRDD表示的评分矩阵
        val userProductsRDD = users.cartesian(products) //空的笛卡尔积
        val preRating = model.predict(userProductsRDD); //获取评分矩阵.

        // 从预测评分矩阵中提取得到用户推荐列表
        val userRecs = preRating.filter(_.rating>0)
            .map(rating => (rating.user,(rating.product,rating.rating)))
            .groupByKey()  //根据userId分组
            .map{
                case(userId,recs) => {
                    UserRecs(userId,recs.toList.sortWith(_._2 > _._2).take(TOP_20).map(x=>Recommendation(x._1,x._2)))
                }
            }.toDF()

        //定义一个隐式参数
        implicit val mongoConf = MongoConfig(config("mongo.uri"),config("mongo.db"))
        storeDataInMongoDB(userRecs,USER_RECS)

        // 3. 利用商品的特征向量，计算商品的相似度列表
        val productFeature = model.productFeatures.map{
            case(productId,features) => (productId,new DoubleMatrix(features))
        }
        // 两两配对商品(笛卡尔积)，
        val productCartesian = productFeature.cartesian(productFeature).filter{
            case(a,b) => (a._1 != b._1) //同一个商品不计算相似度
        }

        //计算余弦相似度
        val productConsinSim = productCartesian.map{
            case(a,b) => {
                val consSim = consinSim(a._2,b._2)
                (a._1,(b._1,consSim))  //返回一个 商品 和另一个商品的余弦相似度
            }
        }

        //商品相似列表
        val productRecs = productConsinSim
            .filter(_._2._2>0.4)    //余弦相似度>0.4
            .groupByKey()
            .map{
                case(productId,recs) => ProductRecs(productId,recs.toList.sortWith(_._2>_._2).map(x=>Recommendation(x._1,x._2)))
            }.toDF()

        //写入mongo
        storeDataInMongoDB(productRecs,PRODUCT_RECS)

        spark.stop()
    }

    def consinSim(product1: DoubleMatrix, product2: DoubleMatrix):Double={
        product1.dot(product2)/(product1.norm2() * product2.norm2())  // dot 点乘
    }


    def storeDataInMongoDB(data: DataFrame, collection_name: String)(implicit mongoConf:MongoConfig): Unit ={
        data.write
            .option("uri",mongoConf.uri)
            .option("collection",collection_name)
            .mode(SaveMode.Overwrite)
            .format("com.mongodb.spark.sql")
            .save()
    }

}

/**
  * 评分表
  * 4867    用户ID
  * 457976  商品ID
  * 5.0     评分
  * 1395676800  时间戳
  */
case class ProductRating(userId: Int,productId: Int,score: Double,timestamp: Int)

/**
  * MongoDB连接配置
  * @param uri    MongoDB的连接uri
  * @param db     要操作的db
  */
case class MongoConfig( uri: String, db: String )

/**
  * 标准推荐对象 Recommendation
  * @param productId    商品ID
  * @param score     推荐分数
  */
case class Recommendation(productId:Int, score:Double)

//用户推荐列表
case class UserRecs(userId:Int,recommendations: Seq[Recommendation])

//商品相似度列表
case class ProductRecs(productId:Int,recommendations: Seq[Recommendation])
