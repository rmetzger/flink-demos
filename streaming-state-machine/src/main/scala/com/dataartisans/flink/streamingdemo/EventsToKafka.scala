package com.dataartisans.flink.streamingdemo

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.DefaultEncoder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.util.DataOutputSerializer
import org.apache.flink.streaming.util.serialization.SerializationSchema

object EventsToKafka {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    // create Kafka producer
    val properties = new Properties
    properties.put("metadata.broker.list", params.getRequired("brokerList"))
    properties.put("serializer.class", classOf[DefaultEncoder].getCanonicalName)
    properties.put("key.serializer.class", classOf[DefaultEncoder].getCanonicalName)

    val config: ProducerConfig = new ProducerConfig(properties)

    val producer = new Producer[Event, Array[Byte]](config)

    val generator = new EventsGenerator()
    val ser = new EventSerializer

    while (true) {
      val nextEvent = generator.next(1, Integer.MAX_VALUE)
      println("Emit event "+nextEvent)
      val serialized = ser.serialize(nextEvent)
      producer.send(new KeyedMessage[Event, Array[Byte]](params.getRequired("topic"), serialized))
    }

  }

  class EventSerializer extends SerializationSchema[Event, Array[Byte]] {
   val byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)

    override def serialize(t: Event): Array[Byte] = {
      byteBuffer.clear()
      byteBuffer.putInt(0, t.sourceAddress)
      byteBuffer.putInt(4, t.event)
      byteBuffer.array()
    }
  }
}
