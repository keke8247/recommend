package com.wdk.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Description
  *             ALS模型评估与参数选取
  * @Author wangdk,wangdk@erongdu.com
  * @CreatTime 2020/2/24 10:07
  * @Since version 1.0.0
  */
object ALSTrainer {
    //定义 mongo Collection
    val MONGO_RATING_COLLECTION = "Rating"

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
            .map(rating => Rating(rating.userId,rating.productId,rating.score))
            .cache()

        // 将一个RDD随机切分成两个RDD，用以划分训练集和测试集
        val splits = ratingRDD.randomSplit(Array(0.8,0.2));
        val trainData = splits(0);
        val testData = splits(1);

        //核心实现 输出最优参数
        adjustAlsParams(trainData,testData)
        spark.stop();
    }

    def adjustAlsParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={
        //遍历数组中定义的参数
        val result = for(rank <- Array(5,10,20,50);lambda <- Array(1,0.1,0.01))
            yield { //用yield关键字收集for循环中的遍历结果
                val model = ALS.train(trainData,rank,5,lambda) //迭代5次
                val rmse = getRMSE(model,testData); //用模型和测试数据 获取rmse
                (rank,lambda,rmse)
        }
        //输出最小的rmse 获取最优参数
        println(result.minBy(_._3))
    }

    //计算均方根误差（RMSE）
    def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]):Double ={
        //构建userProducts 空矩阵
        val userProducts = data.map(item => (item.user,item.product))
        //根据模型得到预测评分矩阵
        val preRating = model.predict(userProducts);

        //首先把预测评分和实际评分表按照(userId, productId)做一个连接
        val observed = data.map(item => ((item.user,item.product),item.rating))
        val predict = preRating.map(item => ((item.user,item.product),item.rating))

        //按照公式计算RMSE 均方根误差是预测值与真实值偏差的平方与观测次数n比值的平方根

        sqrt(observed.join(predict).map{
            case((userId,productId),(actual,pre)) =>{
                val err = actual-pre
                err*err
            }
        }.mean())
    }
}
