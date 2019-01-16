package Streaming_Scocker

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Network {
//  def createConnect()= {
//    Class.forName("com.mysql.jdbc.Driver")
//    DriverManager.getConnection("jdbc:mysql://localhost:3306/testone","root","123456")
//
//
//  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[40]").setAppName("Network")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val lines = ssc.socketTextStream("localhost",8000)
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitonOfRecords => {
        val connection = new Util
          partitonOfRecords.foreach(record => {

            val sql = "insert into wordcount(word, wordcount) values('"+record._1 + "'," + record._2 +")"
               connection.execute(sql)

          })




      })

    })
    ssc.start()
    ssc.awaitTermination()
  }

}
//
//def createConnect()= {
//Class.forName("com.mysql.jdbc.Driver")
//DriverManager.getConnection("jdbc:mysql://localhost:3306/testone","root","123456")
//
//
//}
//val sparkConf = new SparkConf().setMaster("local[5]").setAppName("Network")
//val ssc = new StreamingContext(sparkConf,Seconds(5))
//val lines = ssc.socketTextStream("localhost",8000)
//val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
////    result.foreachRDD(rdd=> {
////      val connection = createConnect()
////      rdd.foreach{ record =>
////        val sql = "insert into wordcount(word, wordcount) values('"+record._1 + "'," + record._2 +")"
////        connection.createStatement().execute(sql)
////      }
////    }
////
////    )
//result.print()
//result.foreachRDD(rdd => {
//rdd.foreachPartition(partitonOfRecords => {
//if(partitonOfRecords.size > 0){
//
//val connection = createConnect()
//
//partitonOfRecords.foreach(record => {
//
//val sql = "insert into wordcount(wordcount(word, wordcount) values('"+record._1 + "'," + record._2 +")"
//connection.createStatement().execute(sql)
//
//})
//connection.close()
//}
//
//
//})
//
//})