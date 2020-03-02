package com.wdk.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Description:
  *              基于物品的协同过滤...收集用户的行为,
  *              如果两个商品有同样的受众（感兴趣的人群），那么它们就是有内在相关性的。
  *              根据用户行为分析商品的相识度.称为 "同现相似度"
  *              公式:
  *                 |Ni ∩ Nj|/根号(Ni * Nj)
  *              Ni 是购买商品 i （或对商品 i 评分）的用户列表，Nj 是购买商品 j 的用户列表。
  * @Author:wang_dk
  * @Date:2020 /2/29 0029 15:18
  * @Version: v1.0
  **/
object ItemCfRecommender {
    //定义MongoDB的Collection
    val MONGO_COLLECTION_RATING = "Rating"

    val ITEM_CF_PRODUCT_RECS = "ItemCfProductRecs"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommend",
            "mongo.db" -> "recommend"
        )

        //定义Spark执行环境
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("Item-CF-recommender")

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        import spark.implicits._

        //加载数据
        val ratingDF = spark.read
                .option("uri",mongoConfig.uri)
                .option("collection",MONGO_COLLECTION_RATING)
                .format("com.mongodb.spark.sql")
                .load()
                .as[ProductRating]
                .map(rating => (rating.userId,rating.productId))   //时间戳并不需要
                .toDF("userId","productId")
                .cache()

        //TODO 核心算法  计算商品间的相识度
        //1.统计出商品被评分的次数
        val productRatingCount = ratingDF.groupBy("productId").count()
        //把商品被评分的次数拼接到ratingDf
        val ratingWithCountDF = ratingDF.join(productRatingCount,"productId")

        //将商品评分按照UserId两两配对 统计出两个商品被同一用户评分的次数
        val joinedDF = ratingWithCountDF.join(ratingWithCountDF,"userId")
                .toDF("userId","product1","count1","product2","count2")
                .filter(item=>item.getInt(1)!=item.getInt(3))

        //把joinedDF作为临时表  方便后面写更复杂的sql进行查询
        joinedDF.createOrReplaceTempView("joinedDf")

        //2.统计商品的相似度
        //2.1 统计两个商品被同一个用户评分的次数
        // cooccurrence: 商品i,j 被同一个用户评分的此时  Ni ∩ Nj
        // count1: 商品i 被评分的次数
        // count2: 商品j 被评分的次数
        val cooccurrenceDF = spark.sql(
            """
              |select product1,product2,
              |count(userId) as cooccurrence,
              |first(count1) as count1,
              |first(count2) as count2
              |from joinedDf
              |group by product1,product2
            """.stripMargin).cache()

        val simDF = cooccurrenceDF.map(item =>{
            val simScore = getSimScore(item.getAs[Long]("cooccurrence"),item.getAs[Long]("count1"),item.getAs[Long]("count2"))
            (item.getAs[Int]("product1"),(item.getAs[Int]("product2"),simScore))
        })
                .rdd
                .groupByKey().map(item =>{
            ProductRecs(item._1,item._2.toArray.sortWith(_._2>_._2).map(x => Recommendation(x._1,x._2)).take(10))
        }).toDF()

        //写入MongoDB
        simDF.write
                .option("uri",mongoConfig.uri)
                .option("collection",ITEM_CF_PRODUCT_RECS)
                .mode(SaveMode.Overwrite)
                .format("com.mongodb.spark.sql")
                .save()

        spark.stop()
    }

    def getSimScore(cooccurrence: Long, count1: Long, count2: Long):Double={
        cooccurrence / math.sqrt(count1*count2)
    }

}

/**
  * 标准推荐对象 Recommendation
  * @param productId    商品ID
  * @param score     推荐分数
  */
case class Recommendation(productId:Int, score:Double)

//商品相似度列表
case class ProductRecs(productId:Int,recommendations: Seq[Recommendation])

/**
  * MongoDB连接配置
  * @param uri    MongoDB的连接uri
  * @param db     要操作的db
  */
case class MongoConfig( uri: String, db: String )

/**
  * 评分表
  * 4867    用户ID
  * 457976  商品ID
  * 5.0     评分
  * 1395676800  时间戳
  */
case class ProductRating(userId: Int,productId: Int,score: Double,timestamp: Int)
