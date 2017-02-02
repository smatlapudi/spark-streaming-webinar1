package com.test.demo.FileStreamReader

import org.rogach.scallop.ScallopConf
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by smatlapudi on 01/23/17.
  */

class Conf(args: Array[String]) extends ScallopConf(args) {
  import org.rogach.scallop.singleArgConverter

  val batchSize = opt[Int]("batch-interval", 'b', descr = "Stream Batch Size in seconds", default = Some(3*60))
  val hdfsInputDataDir = opt[String]("hdfs-input-dir", 'd', descr = "HDFS input Directory ", required=true)
  val hdfsOutputDataDir = opt[String]("hdfs-output-dir", 'd', descr = "HDFS output Directory ", required=true)
  verify()
}


object KafkaReaderMain extends Logging {

  def main(args: Array[String]) {

    //parser options
    val opts = new Conf(args)
    val batchSize = opts.batchSize()
    val hdfsInputDataDir = opts.hdfsInputDataDir()
    val hdfsOutputDataDir = opts.hdfsOutputDataDir()


    logInfo(s"\t> batchSize          :$batchSize")
    logInfo(s"\t> hdfsInputDataDir   :$hdfsInputDataDir")
    logInfo(s"\t> hdfsInputDataDir   :$hdfsOutputDataDir")


    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    //create  new streaming context
    val ssc = new StreamingContext(sc, Seconds(batchSize))


    // Read files from hdfsInputDataDir
    val inputLineStream = ssc.textFileStream(hdfsInputDataDir)

    // Filter for lines with hadoop sunstring
    val outputLineStream =  inputLineStream.filter(_.contains("hadoop"))

    // Save Output Lines to hdfsOutputDataDir
    outputLineStream.saveAsTextFiles(s"$hdfsOutputDataDir/lines")


    //start the steaming context and wait for termination
    ssc.start
    ssc.awaitTermination
  }

}
