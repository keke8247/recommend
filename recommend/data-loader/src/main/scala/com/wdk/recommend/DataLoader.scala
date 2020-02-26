package com.wdk.recommend

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 商品信息表
  * 3982                                    商品ID
  * Fuhlen 富勒 M8眩光舞者时尚节能无线鼠标(草绿)  商品名称
  * 1057,439,736                            商品分类ID(不需要)
  * B009EJN4T2                              亚马逊ID(不需要)
  * https://images-cn-4                     图片URL
  *  外设产品|鼠标|电脑/办公                    商品分类
  * 富勒|鼠标|电子产品|好用|外观漂亮             商品标签
  */
case class Product(productId:Int,name:String,imageUrl:String,categories:String,tags:String);

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

/**
  * @Description:
  *              数据加载器
  * @Author:wang_dk
  * @Date:2020 /2/22 0022 10:33
  * @Version: v1.0
  **/
object DataLoader {

    //数据文件路径
    val PRODUCT_DATA_PATH = "D:\\files\\program\\idea\\recommend-system\\recommend\\data-loader\\src\\main\\resources\\products.csv";
    val RATING_DATA_PATH = "D:\\files\\program\\idea\\recommend-system\\recommend\\data-loader\\src\\main\\resources\\ratings.csv";

    //定义mongo中的collection名称
    val MONGO_PRODUCT_COLLECTION = "Product"
    val MONGO_RATING_COLLECTION = "Rating"


    def main(args: Array[String]): Unit = {
        //创建配置项
        val config = Map(
            "spark.cores" -> "local[*]",    //spark计算的时候占用所有CPU核数
            "mongo.uri" -> "mongodb://localhost:27017/recommend",   //mongo路径
            "mongo.db" -> "recommend"
        )


        //定义sparkConf
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader");
        val spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate() //getOrCreate 如果有sparkSession了 就直接用,如果没有就创建一个

        import spark.implicits._

        //加载数据
        val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH);

        val productDF = productRDD.map(item=>{
            val attr = item.split("\\^")

            //转成Product
            Product(attr(0).toInt,attr(1).trim,attr(4).trim,attr(5).trim,attr(6).trim);
        } ).toDF();


        val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH);

        val ratingDF = ratingRDD.map(item=>{
            val attr = item.split(",");

            Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
        }).toDF()

        //隐式变量
        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"));

        storeDataInMongoDB(productDF,ratingDF);
    }

    //数据持久化到数据库 mongoConfig 为隐式参数 传入一次就行了
    def storeDataInMongoDB(productDF:DataFrame,ratingDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit ={
        //创建 mongo的连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

        //定义要操作的mongo collection
        val productCollection = mongoClient(mongoConfig.db)(MONGO_PRODUCT_COLLECTION)
        val ratingCollection = mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION)

        //如果Collection已存在 先删除
        productCollection.dropCollection();
        ratingCollection.dropCollection();

        //写入数据  用这种写入方式 其实不需要构建 productCollection
        productDF.write
                .option("uri",mongoConfig.uri)
                .option("collection",MONGO_PRODUCT_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        ratingDF.write
                .option("uri",mongoConfig.uri)
                .option("collection",MONGO_RATING_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()


        //创建索引
        productCollection.createIndex(MongoDBObject("productId"->1))
        ratingCollection.createIndex(MongoDBObject("productId"->1))
        ratingCollection.createIndex(MongoDBObject("userId"->1))

        //关闭资源
        mongoClient.close()

    }

}
