package com.wdk.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @Description
  *             离线推荐 根据商品评分表
  *             统计出:
  *                 1:评分数最多的商品
  *                 2:近期评分最多的商品
  *                 3:商品的平均评分
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/2/23 15:19
  * @Since version 1.0.0
  */
object StatisticsRecommender {

    //定义 mongo Collection
    val MONGO_RATING_COLLECTION = "Rating"

    val RATE_MORE_PRODUCTS = "RateMoreProducts" //评分数量表

    val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts" //近期评分数据

    val AVERAGE_PRODUCTS = "averageProducts" //商品平均分

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
        val ratingDF = spark.read
            .option("uri",config("mongo.uri"))
            .option("collection",MONGO_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Rating]
            .toDF()

        //把基础ratingDF数据做为一个临时表
        ratingDF.createOrReplaceTempView("ratings")

        //1:评分数最多的商品 使用ratings临时表
        val rateMoreProductsDF = spark.sql("select productId,count(productId) as count from ratings group by productId order by count desc");

        //定义一个隐式参数
        implicit val mongoConf = MongoConfig(config("mongo.uri"),config("mongo.db"))

        //把数据写回MongoDB
        storeDataInMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCTS);

        //2:近期评分最多的商品 要根据yyyyMM格式转换时间戳.
        //2.1定义时间格式format
        val dateFormat = new SimpleDateFormat("yyyyMM")
        //2.2 注册udf
        spark.udf.register("changeDate",((x:Int)=>{
            dateFormat.format(new Date(x*1000L)).toInt
        }))
        //2.3获取转换时间后的中间数据
        val ratingOfYearMonthDF = spark.sql("select productId,score,changeDate(timestamp) as yearmonth from ratings");
        ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")

        //2.4从临时表ratingOfMonth统计数据 根据month 和 product 分组,并写回到mongo
        val rateMoreRecentlyProductsDF = spark.sql("select productId,count(1) as count,yearmonth from ratingOfMonth group by yearmonth,productId order by yearmonth desc,count desc")
        storeDataInMongoDB(rateMoreRecentlyProductsDF,RATE_MORE_RECENTLY_PRODUCTS)

        //3:商品平均分
        val averageProductsDF = spark.sql("select productId,avg(score) as avg from ratings group by productId order by avg desc")
        storeDataInMongoDB(averageProductsDF,AVERAGE_PRODUCTS);

        spark.stop()
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
case class Rating(userId: Int,productId: Int,score: Double,timestamp: Int)

/**
  * MongoDB连接配置
  * @param uri    MongoDB的连接uri
  * @param db     要操作的db
  */
case class MongoConfig( uri: String, db: String )
