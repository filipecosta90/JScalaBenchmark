package com.fcosta_oliveira

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.openjdk.jmh.Main
import org.slf4j.{Logger, LoggerFactory}

class RowSerializerClassAndObject(var s:StructType)  extends Serializer[Row] {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Main])

  var schema = s

  // TODO: assess with Oleksiy (@fe2s) if the datatTypes are all covered
  override def write(kryo: Kryo, output: Output, t: Row): Unit = {
    // write the number of fields
    output.writeInt(t.size)
    for (i <- 0 to t.length - 1) {
      kryo.writeClassAndObject(output, t.get(i))
    }
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[Row]): Row = {
    val size = input.readInt()
    val cols = new Array[Any](size)

    for (fieldnum <- 0 to size - 1) {
      val obj: AnyRef = kryo.readClassAndObject(input)
      cols(fieldnum) = obj
    }

    var newRow: Row = new GenericRowWithSchema( cols, schema )
    newRow
  }
}
