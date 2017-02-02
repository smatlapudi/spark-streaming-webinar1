package com.test.demo.kafkaReader

import org.rogach.scallop.ScallopConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{Logging, SparkConf, SparkContext}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

/**
  * Created by smatlapudi on 01/23/17.
  */

class Conf(args: Array[String]) extends ScallopConf(args) {
  import org.rogach.scallop.singleArgConverter

  val kafkaBrokers = opt[String]("kafka-brokers", 'k', descr = "Kafka Broker List (comma separated)", required=true)
  val kafkaTopic = opt[String]("kafka-topic", 't', descr = "Kafka Topic")
  val batchSize = opt[Int]("batch-interval", 'b', descr = "Stream Batch Size in seconds", default = Some(3*60))
  val hdfsDataDir = opt[String]("hdfs-data-dir", 'd', descr = "HDFS Directory for output data ", required=true)
  val hdfsCheckPointDir = opt[String]("hdfs-checkpoint-dir", 'c', descr = "HDFS Directory for spark checkpointing", required=true)
  verify()
}


object KafkaReaderMain extends Logging {

  def main(args: Array[String]) {

    //parser options
    val opts = new Conf(args)
    val kafkaTopic = opts.kafkaTopic()
    val kafkaBrokers = opts.kafkaBrokers()
    val batchSize = opts.batchSize()
    val hdfsDataDir = opts.hdfsDataDir()
    val hdfsCheckPointDir = opts.hdfsCheckPointDir()

    val kafkaTopics = Set(kafkaTopic)

    logInfo(s"\t> batchSize           :$batchSize")
    logInfo(s"\t> kafkaTopics         :$kafkaTopic")
    logInfo(s"\t> kafkaBrokers        :$kafkaBrokers")
    logInfo(s"\t> hdfsDataDir         :$hdfsDataDir")
    logInfo(s"\t> hdfsCheckPointDir   :$hdfsCheckPointDir")


    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    // function to create and setup new streaming context
    val createStreamingContext = () => {

      //create  new streaming context
      val ssc = new StreamingContext(sc, Seconds(batchSize))

      //create a kafka direct stream
      val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)

      val kafkaEvents: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)

      // Reparition to number of core 
      // to avoid too many output files - this is optional
      val outputEvents = kafkaEvents.map{ case(k,v) => v }.repartition(sc.defaultParallelism)

      outputEvents.saveAsTextFiles(s"$hdfsDataDir/events")

      // set checkpoint
      ssc.checkpoint(hdfsCheckPointDir)

      //finally return the streaming context
      ssc
    }

    // StreamingContext from checkpoint data or create a new one
    val ssc = StreamingContext.getOrCreate(hdfsCheckPointDir, createStreamingContext)

    //start the steaming context and wait for termination
    ssc.start
    ssc.awaitTermination
  }

}
