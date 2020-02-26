package com.wdk.online

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @Description
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/2/24 20:42
  * @Since version 1.0.0
  */
object OnLineRecommender {
    //定义MongoDB的 collection
    val PRODUCT_RECS = "ProductRecs"

    val MONGO_RATING_COLLECTION = "Rating"

    val STREAM_RECS = "StreamRecs"

    val MAX_USER_RATING_NUM = 20 //用户的最近20条评分记录 从Redis中获取

    val MAX_SIM_PRODUCTS_NUM = 20 //商品相似度最高的前20条记录 从商品相似度矩阵中获取

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores"->"local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommend",
            "mongo.db" -> "recommend",
            "kafka.topic" -> "recommender"
        )

        //定义sparkConf
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
        //定义sparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate();
        //定义StreamingContext
        val ssc = new StreamingContext(spark.sparkContext,Seconds(2))

        //step1 加载 商品相似度矩阵 ProductRecs数据
        val simProductsMatrix = spark.read
            .option("uri",config("mongo.uri"))
            .option("collection",PRODUCT_RECS)
            .format("com.mongodb.spark.sql")
            .load()
            .as[ProductRecs]
            .rdd
            //为了后续查询方便,转换成map格式Map<productId:Map<productId:score>>. 二元组可以直接转map
            .map(item=>{
                    (item.productId,item.recommendations.map(x=>(x.productId,x.score)).toMap)
                }
            ).collectAsMap()

        // 由于很多地方都要用到 商品相似度矩阵,所以把这部分数据定义为广播变量
        val simProductsMatrixBC = spark.sparkContext.broadcast(simProductsMatrix)

        //step 2接收从kafka推送过来的 用户实时评分数据 数据格式定义为 userId|productId|score|timestamp
        // 2.1 定义kafka配置
        val kafkaConfig = Map(
            "bootstrap.servers" -> "master:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "recommender",
            "auto.offset.reset" -> "latest"
        )
        // 2.2 创建DStream
        val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaConfig))

        // 2.3接收kafkaStream数据 并进行转换
        val streamRating = kafkaStream.map(msg=>{
            val attr = msg.value().split("\\|");
            (attr(0).trim.toInt,attr(1).trim.toInt,attr(2).trim.toDouble,attr(3).trim.toInt)
        })

        //TODO 核心算法 定义评分流的处理流程
        streamRating.foreachRDD(
            rdds => rdds.foreach{ // rdds 是一段时间的评分数据 是多条记录
                case(userId,productId,score,timestamp) => {
                    println("rating data comming >>>>>>>>>>>>>>>>>>>>>>>>")
                    //step3 从redis里取出当前用户的最近评分，保存成一个数组Array[(productId, score)]
                    val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATING_NUM,userId,ConnectionHelper.jedis);

                    //step4 从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表，保存成一个数组Array[productId]
                    //定义一个隐式参数
                    implicit val mongoConf = MongoConfig(config("mongo.uri"),config("mongo.db"))
                    val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM,productId,userId,simProductsMatrixBC.value)

                    //step5 计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表，保存成 Array[(productId, score)]
                    val streamRecs = computeProductScore(userRecentlyRatings,candidateProducts,simProductsMatrixBC.value)

                    //step6 写入mongoDB
                    saveDataToMongoDB(userId,streamRecs)
                }
            }
        )
    }
    import scala.collection.JavaConversions._ //把java中集合类转成scala的 就可以使用一些 map filter函数了
    //从Redis中获取数据
    def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis):Array[(Int,Double)]={
        // 从redis中用户的评分队列里获取评分数据，list键名为uid:USERID，值格式是 PRODUCTID:SCORE
        jedis.lrange("userId:"+userId,0,num) //lrange Lrange 返回列表中指定区间内的元素
            .map { item =>
                val attr = item.split("\\:")
                (attr(0).trim.toInt, attr(1).trim.toDouble)
            }.toArray
    }

    //从相似度矩阵中获取 与当前商品相似的前N条商品 过滤掉用户已经评分过的商品
    def getTopSimProducts(num: Int, productId: Int, userId: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                         (implicit mongoConfig : MongoConfig):Array[Int] ={
        //获取用户已经评分过的商品 从MongoDB 的 Rating collection中获取
        val mongoClient = ConnectionHelper.mongoClient;
        val ratingCollection = mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION)

        val ratingExist = ratingCollection.find(MongoDBObject("userId"->userId))
            .map(item=>item.get("productId").toString.toInt
        )

        //获取相似商品列表
        val allSimProducts = simProducts(productId).toArray

        allSimProducts
            .filter(item=> !ratingExist.contains(item._1))  //过滤已经有过评分的商品
            .sortWith(_._2>_._2)    //根据相似度排序
            .take(num)              //获取topN
            .map((_._1))            //取productId
    }

    //计算每个备选商品的推荐得分
    //计算公式: Euq = (sim(q,r)*R)/sim_sum + lg max{incount,1} - lg max{recount,1}
    def computeProductScore(userRecentlyRatings: Array[(Int, Double)],
                            candidateProducts: Array[Int],
                            simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]):Array[(Int,Double)]={
        // 定义一个长度可变数组ArrayBuffer，用于保存每一个备选商品的基础得分，(productId, score)
        val scores = new ArrayBuffer[(Int,Double)]()
        // 定义两个map，用于保存每个商品的高分和低分的计数器，productId -> count
        val increMap = new mutable.HashMap[Int,Int]()
        val decreMap = new mutable.HashMap[Int,Int]()

        // 遍历每个备选商品，计算和已评分商品的相似度
        for(candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings){
            // 从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
            val simScore = getProductsSimScore( candidateProduct, userRecentlyRating._1, simProducts )
            if(simScore >0.4){  //相似度小于0.4的 没有推荐价值 丢弃
                // 按照公式进行加权计算，得到基础评分
                // sim(q,r):表示商品q和商品r的相似度  sim(q,r) = simScore
                // R 用户对商品r的评分
                scores += ((candidateProduct,simScore*userRecentlyRating._2))

                if(userRecentlyRating._2 > 3){  //如果用户对商品评分>3分 高分计数器+1
                    increMap(candidateProduct) = increMap.getOrDefault(candidateProduct,0) + 1
                }else{
                    decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct,0) + 1
                }
            }
        }
        // 根据公式计算所有的推荐优先级，首先以productId做groupby
        scores.groupBy(_._1).map{
            case(productId,scores) =>
                (productId,scores.map(_._2).sum/scores.length+ log(increMap.getOrDefault(productId,1)) - log(decreMap.getOrDefault(productId,1)))
        }.toArray
            .sortWith(_._2>_._2)
    }

    // 从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
    def getProductsSimScore(product1: Int, product2: Int,
                            simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]):Double={
        simProducts.get(product1) match {   //从相似度矩阵中获取 备选商品的相似度列表
            case Some(sims) => sims.get(product2) match {   //如果列表有值  从里面匹配已评分商品
                case Some(score) => score   //匹配上 则返回分数
                case None => 0.0    //匹配不上 说明没有关联性 返回0.0
            }
            case None => 0.0
        }
    }
    // 自定义log函数，以N为底
    def log(m: Int):Double={
        val N = 10;
        math.log(m)/math.log(N) // = N为底 m的对数
    }

    // 写入mongodb
    def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
        val streamRecsCollection = ConnectionHelper.mongoClient(mongoConfig.db)(STREAM_RECS)
        // 按照userId查询并更新
        streamRecsCollection.findAndRemove( MongoDBObject( "userId" -> userId ) )
        streamRecsCollection.insert( MongoDBObject( "userId" -> userId,
            "recs" -> streamRecs.map(x=>MongoDBObject("productId"->x._1, "score"->x._2)) ) )
    }
}



// 定义一个连接助手对象，建立到redis和mongodb的连接
object ConnectionHelper extends Serializable {
    // 懒变量定义，使用的时候才初始化
    lazy val jedis = new Jedis("localhost")
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommend"))
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
