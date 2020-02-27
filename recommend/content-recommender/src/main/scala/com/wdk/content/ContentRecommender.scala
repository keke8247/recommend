package com.wdk.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jblas.DoubleMatrix

/**
  * @Description:
  *              基于商品内容的相似推荐
  * @Author:wang_dk
  * @Date:2020 /2/27 0027 22:03
  * @Version: v1.0
  **/
object ContentRecommender {
    //定义mongo中的collection名称
    val MONGO_PRODUCT_COLLECTION = "Product"

    val CONTENT_PRODUCT_RECS = "ContentProductRecs"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommend",
            "mongo.db" -> "recommend"
        )

        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("content-recommender")

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        //加载数据
        val productDF = spark.read
                .option("uri",mongoConfig.uri)
                .option("collection",MONGO_PRODUCT_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Product]
                .map(item=>
                    (item.productId,item.name,
                            item.tags.map(c => if(c == '|')  ' ' else c)) //把 tags里面的 | 线 转换为 空格' ' 注意这里是单引号 字符
                )
                .toDF("productId","name","tags").cache()

        //实例化一个分词器  默认按空格分词  指定要切分的列 和输出列
        val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")

        //用分词器做转换
        val worksData = tokenizer.transform(productDF)

        //定义一个HashingTf工具
        val hashintTf = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)

        //统计TF(词频)  用HashingTf做 处理
        val featurizedData = hashintTf.transform(worksData)

        //定义一个IDF工具
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

        //将词频数据传入 得到IDF模型
        val idfMode = idf.fit(featurizedData)

        // 用TF-IDF算法得到新的矩阵特征
        val rescaledData = idfMode.transform(featurizedData)

        // 从特征矩阵中提取特征向量
        val productFeature = rescaledData.map{
            case row => (row.getAs[Int]("productId"),row.getAs[SparseVector]("features").toArray)
        }.rdd.map( x=> (x._1,new DoubleMatrix(x._2)))

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
        storeDataInMongoDB(productRecs,CONTENT_PRODUCT_RECS)

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
  * MongoDB连接配置
  * @param uri    MongoDB的连接uri
  * @param db     要操作的db
  */
case class MongoConfig( uri: String, db: String )

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
  * 标准推荐对象 Recommendation
  * @param productId    商品ID
  * @param score     推荐分数
  */
case class Recommendation(productId:Int, score:Double)


//商品相似度列表
case class ProductRecs(productId:Int,recommendations: Seq[Recommendation])
