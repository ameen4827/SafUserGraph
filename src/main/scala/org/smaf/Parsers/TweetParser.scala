package org.smaf.Parsers
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType, LongType, StringType, DoubleType }
import org.apache.spark.sql.functions._
import org.graphframes._
object TweetParser {
  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("User Network Graph")
    //val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")
    //creates Spark Session
   // val spark = SparkSession.builder().appName("User Network Graph").getOrCreate()
   
     val spark = SparkSession.builder()
    .appName("User Network Graph")
    .master("yarn").getOrCreate()
    
    import spark.implicits._
    //tweets folder address on HDFS 
    val inputpath = "hdfs:///data/json/tweet/kashmir.json"
    val outputpath = "hdfs:///data/json/output/" 
    //get the raw tweets from HDFS
    val raw_tweets = spark.read.format("json").option("inferScehma", "true").option("mode", "dropMalformed").load(inputpath)
   //Reply To
    var tweetsTmp1=raw_tweets.select(functions.col("id"),functions.col("user_id").cast(LongType).as("user_id"),functions.col("username").as("username"),raw_tweets("created_at").cast(LongType).as("created_at"), explode(functions.col("reply_to")).as("reply_to"),functions.col("mentions").as("mentions"),functions.col("hashtags").as("hashtags")).sort("created_at")
    tweetsTmp1=tweetsTmp1.select(functions.col("id"),col("user_id"),col("username"),col("created_at"),col("reply_to.user_id").as("replyToUserId") ,col("reply_to.username").as("replyToUsername"),col("mentions"),col("hashtags")).where(col("username")=!=col("replyToUsername"))
    var pairReplyTo=tweetsTmp1.select(col("username").as("src"),col("replyToUsername").as("dst")).withColumn("realtionship", lit("replyto"))
    
    // mentions
    var tweetsTmp2=raw_tweets.select(functions.col("id"),functions.col("user_id").cast(LongType).as("user_id"),functions.col("username").as("username"),functions.col("created_at").cast(LongType).as("created_at"), functions.col("reply_to"),explode(functions.col("mentions")).as("mentions"),functions.col("hashtags").as("hashtags")).sort("created_at")
    var pairMentions=tweetsTmp2.select(col("username").as("src"),concat(lit("m_"),col("mentions")).as("dst")).withColumn("realtionship", lit("mentions"))
    //Hashtags
    var tweetsTmp3=raw_tweets.select(functions.col("id"),functions.col("user_id").cast(LongType).as("user_id"),functions.col("username").as("username"),functions.col("created_at").cast(LongType).as("created_at"), functions.col("reply_to"),functions.col("mentions").as("mentions"),explode(functions.col("hashtags")).as("hashtags")).sort("created_at")
    var pairHashtags=tweetsTmp3.select(col("username").as("src"),translate(col("hashtags"),"#","h_").as("dst")).withColumn("realtionship", lit("hashtag"))
    //Unionall pairs
    
    var pairUnined=pairReplyTo.union(pairMentions).union(pairHashtags)
    pairUnined=pairUnined.distinct
    //Vertex dataframe
   var vSrc= pairUnined.select(col("src")).distinct
   var vdst= pairUnined.select(col("dst")).distinct
   var v=vSrc.union(vdst).select(col("src").as("id"))
    
    var e=pairUnined
    //Creating GraphFrame
    val g = GraphFrame(v, e)
    
    // Run PageRank until convergence to tolerance "tol".
    val results = g.pageRank.resetProbability(0.15).tol(0.01).run()
    
    //saving the pagerank in HDFS in the format of ID Username PageRank.
   // pairUnined.rdd.repartition(1).saveAsTextFile(outputpath.concat("Edgelist_"+System.currentTimeMillis))
    
    pairUnined.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", " ").save(outputpath.concat("Edgelist_"+System.currentTimeMillis))
    spark.close()
  }
}