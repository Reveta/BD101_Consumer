package com.epam.streaming

object Main extends App {
    val topic = args(0)
    val brokers = "sandbox-hdp.hortonworks.com:6667"
    val groupid = "consumer-1"

    new Consumer(brokers,groupid,topic).run()
    SparkJob.sparkJob(SparkConfig.spark)
}
