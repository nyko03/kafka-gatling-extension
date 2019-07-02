package io.gatling.simulation

import java.util

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.gatling.core.Predef._
import io.gatling.data.generator.RandomDataGenerator
import io.gatling.kafka.{KafkaProducerBuilder, KafkaProducerProtocol}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerConfig

class SimulationWithAvroSchema extends Simulation {
  val kafkaTopic = "test_topic_avro"
  val kafkaBrokers = "kafka-1:9092,kafka-2:9093"

  val props = new util.HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put("schema.registry.url", "http://schema-registry:8081")

  val user_schema =
    s"""
       | {
       |    "fields": [
       |        { "name": "int1", "type": "int" }
       |    ],
       |    "name": "myrecord",
       |    "type": "record"
       |}
     """.stripMargin

  val schema = new Schema.Parser().parse(user_schema)

  val dataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]()
  val kafkaProducerProtocol = new KafkaProducerProtocol[GenericRecord, GenericRecord](props, kafkaTopic, dataGenerator)
  val scn = scenario("Kafka Producer Call").exec(KafkaProducerBuilder[GenericRecord, GenericRecord](Some(schema)))

  constantUsersPerSec(100000) during (60000)
  setUp(scn.inject(atOnceUsers(1))).protocols(kafkaProducerProtocol)
}