import java.util.Properties
import java.util.concurrent.Future

import com.szcentral.fusedgoods.kafka.tool.ScalaKafkaProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Author: yanghaijun
  * Description: 
  * Date: Crated in 10:26 2017/12/15
  * Modified By: 
  */
class ProduceKeyedMsg extends App {
  def sendStr(topic: String, key: String, msg: String): Unit = {
    //创建一个需要发送的pro
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ScalaKafkaProperties.BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //创建一个生产者
    val producer = new KafkaProducer[String, String](props)

    val ret: Future[RecordMetadata] = producer.send(new ProducerRecord(topic, key, msg))
    val metadata = ret.get // 打印出 metadata
    println("offset=" + metadata.offset() + ",  partition=" + metadata.partition())
    //关闭生产者
    producer.close()
  }
}
