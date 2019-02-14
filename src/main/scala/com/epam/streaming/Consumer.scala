package com.epam.streaming


import java.util.concurrent._
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConversions._

class zConsumer(val brokers: String,
               val groupId: String,
               val topic: String) {

  val props: Properties = createConsumerConfig(brokers, groupId)
  val kafkaConsumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null


  def shutdown(): Unit = {
    if (kafkaConsumer != null)
      kafkaConsumer.close()
    if (executor != null)
      executor.shutdown()
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-1")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    return props
  }

  def run(): Unit = {
    kafkaConsumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, String] = kafkaConsumer.poll(500)

          for (record <- records) {
            System.out.println("Received message: (" + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }
}

object Consumer extends App {
  var topic = args(0)
  val newArgs = Array("sandbox-hdp.hortonworks.com:6667", "consumer-1", topic)
  val example = new Consumer(newArgs(0), newArgs(1), newArgs(2))
  example.run()
  SparkJob.sparkJob()
}