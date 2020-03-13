package org.smaf.Parsers
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType, LongType, StringType, DoubleType }
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.spark.sql._
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger

import collection.JavaConverters._
object EsTweetParser {
  val log = Logger.getLogger(getClass.getName)
  case class Edge(src: String, dst: String, realtionship: String)
  case class Node(name: String)
  case class Link(source: String, target: String, realtionship: String)
  case class Graph(reqId: String, nodes: Seq[Node], links: Seq[Link])

  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("User Network Graph")
    //val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")
    //creates Spark Session
    // val spark = SparkSession.builder().appName("User Network Graph").getOrCreate()
    var strKeyword = "";
    var strType = "";
    var strReqId = "";

    if (args.length > 0) {

      strType = args(0).toString()
      strKeyword = args(1).toString()
      strReqId = args(2).toString()

      val spark = SparkSession.builder()
        .appName("User Network Graph_" + strReqId)
        .config("spark.mongodb.input.uri", "mongodb://mongo/socio.d3raw")
        .config("spark.mongodb.output.uri", "mongodb://mongo/socio.d3raw")
        .master("yarn").getOrCreate()

      import spark.implicits._
      var reader = spark.read.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true").option("es.nodes.wan.only", "false").option("es.port", "9200").option("es.net.ssl", "false").option("es.nodes", "http://10.30.9.25").option("es.mapping.date.rich", "false").option("spark.es.mapping.date.rich", "false")

      var q = s"""{ "query":{ "multi_match" : { "query":"$strKeyword", "fields": [ "username", "name","place","mentions","hashtags" ] } } }"""

      //get the raw tweets from HDFS
      val raw_tweets = reader.option("es.query", q).option("es.read.field.as.array.include", "mentions,hashtags").option("es.read.field.as.array.exclude", "reply_to").load("keywordtweets").select("id", "user_id_str", "username", "name", "place", "reply_to", "mentions", "hashtags", "created_at")

      //Reply To
      var tweetsTmp1 = raw_tweets.withColumn("exp_reply_to", explode(functions.col("reply_to"))).withColumn("realtionship", lit("replyto")).withColumn("replyToUsername", col("exp_reply_to.username")).withColumn("replyToUserId", col("exp_reply_to.user_id")).sort("created_at")
      var pairReplyTo = tweetsTmp1.withColumn("src", col("username")).withColumn("dst", col("replyToUsername")).where(col("username") =!= col("replyToUsername")).drop("exp_reply_to")

      // var pairReplyTo=tweetsTmp1.select(col("username").as("src"),col("replyToUsername").as("dst"))

      // mentions
      var tweetsTmp2 = raw_tweets.withColumn("exp_mentiond", explode(functions.col("mentions"))).sort("created_at")
      var pairMentions = tweetsTmp2.withColumn("src", col("username")).withColumn("dst", concat(lit("m_"), col("exp_mentiond"))).withColumn("realtionship", lit("mentions")).withColumn("replyToUsername", lit(null)).withColumn("replyToUserId", lit(null)).drop("exp_mentiond")
      //Hashtags
      var tweetsTmp3 = raw_tweets.withColumn("exp_hashtags", explode(functions.col("hashtags"))).sort("created_at")
      var pairHashtags = tweetsTmp3.withColumn("src", col("username")).withColumn("dst", translate(col("exp_hashtags"), "#", "h_")).withColumn("realtionship", lit("hashtag")).withColumn("replyToUsername", lit(null)).withColumn("replyToUserId", lit(null)).drop("exp_hashtags")
      //Unionall pairs

      var pairUnined = pairReplyTo.union(pairMentions).union(pairHashtags)
      pairUnined = pairUnined.distinct
      //Vertex dataframe
      var vSrc = pairUnined.select(col("src")).distinct
      var vdst = pairUnined.select(col("dst")).distinct
      var v = vSrc.union(vdst).select(col("src").as("id"))

      var e = pairUnined
      //Creating GraphFrame
      val g = GraphFrame(v, e)
      log.info("***********************Graph Created ****************************")
      g.persist(StorageLevel.MEMORY_AND_DISK_SER)
      log.info("***********************Graph Persisted ****************************")
      log.info("***********************Running Page rank ****************************")
      // Run PageRank for a fixed number of iterations.
      var results2 = g.pageRank.resetProbability(0.15).maxIter(5).run()
      log.info("***********************complete Page rank ****************************")
      //vertices.limit(50).repartition(1).write.json("hdfs:///data/graph/vertices/"+System.currentTimeMillis+"/vertices.json")
      // val vertexes: Array[String] = verticesRank.select("id").rdd.map(x => x(0).toString).collect()

      var edgesRank = results2.edges.drop("reply_to", "mentions", "hashtags")
      edgesRank = edgesRank.distinct().sort(desc("created_at"), desc("weight")).limit(500)

      //Adding request id

      edgesRank = edgesRank.withColumn("reqId", lit(strReqId.toString()))

      // val edges: Array[Array[String]] = edgesRank.select("src", "dst").rdd.map(r => Array(r(0).toString, r(1).toString)).collect()

      //val edgeCreation = edges.map{ edgeArray =>"{source:nodes["+ vertexes.indexOf(edgeArray(0).trim()) +"],target:nodes["+ vertexes.indexOf(edgeArray(1).trim())+"]}"}

      // val edgeCreationtest = edges.map { edgeArray => "{source:nodes[" + edgeArray(0).trim() + "],target:nodes[" + edgeArray(1).trim() + "]}" }
      // egdes.limit(10).repartition(1).write.json("hdfs:///data/graph/egdes/"+System.currentTimeMillis+"/egdes.json")

      /*  val EdgeEncoder = org.apache.spark.sql.Encoders.product[Edge]
    var t = edgesRank.as(EdgeEncoder)
    val data = t.collect()
    val nodes = (data.map(_.src) ++ data.map(_.dst)).map(_.replaceAll("_", " ")).toSet.toSeq.map(Node)
    val links = data.map { t => Link( t.src, t.dst, t.realtionship ) }
    var seqGraph=Seq(Graph(u.toString(),nodes, links))
    val rawD3str=seqGraph.toDF()
    val linkdf=Seq(links).toDF()*/
      //val rawD3str=seqGraph.toDF().toJSON.first()
      log.info("***********************Graph Json Prepared ****************************")

      log.info("***********************Writing to mongodb  ****************************")
      // Writing data in MongoDB:

      MongoSpark.write(edgesRank).option("spark.mongodb.output.uri", "mongodb://mongo/socio").option("collection", "d3raw").mode("append").save()
      //val writeConfig = WriteConfig(Map("uri" -> "mongodb://mongo:27017",  "database" -> "socio", "collection" -> "d3raw"), Some(WriteConfig(spark)))

      //val readConfig = ReadConfig(Map("uri" -> "mongodb://mongo:27017",  "database" -> "socio", "collection" -> "d3raw"), Some(ReadConfig(spark)))
      // MongoSpark.save(rawD3str,writeConfig)
      // rawD3str.write.format("com.mongodb.spark.sql.DefaultSource").option("collection", "d3raw").option("spark.mongodb.output.uri", "mongodb://mongo/socio" ).mode("append").save()
      spark.close()
    }
  }

}