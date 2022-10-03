import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util
import java.util.regex.Pattern
import scala.collection.JavaConverters._



object RDD_ML_Handler extends App{

  val props:Properties = new Properties()
  props.put("group.id", "ML_Handler")

  //props.put("max.poll.records", 5)
  props.put("bootstrap.servers","localhost:9092")

  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  //props.put("auto.offset.reset", "latest")
  //props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)

  val topics = "iris_input_RDD"
  val topicPartition0 = new TopicPartition(topics, 0)

  try {
    //consumer.subscribe(topics.asJava)
    consumer.assign(util.Arrays.asList(topicPartition0))
    //val records = consumer.poll(Duration.ofSeconds(100))
    val topicPartitions = consumer.assignment()
    consumer.seekToBeginning(topicPartitions)
    //consumer.seek(topicPartition0, offset)

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(100))
      println(s"size = ${records.asScala.size}")

      for (record <- records.asScala) {
        val recval = record.value().toString.split('|').toSeq
        //val listrec = recval.split('|').toSeq
        //println(listrec.getClass)
        println(recval)
        //println(listrec)

        /*println("Topic: " + record.topic() +
          ",Key: " + record.key() +
          ",Value: " + record.value() +
          ", Offset: " + record.offset() +
          ", Partition: " + record.partition())*/
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
  println("end")



}
