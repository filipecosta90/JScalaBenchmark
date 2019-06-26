package com.fcosta_oliveira

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.openjdk.jmh.Main
import org.slf4j.{Logger, LoggerFactory}

class RowSerializer extends Serializer[Row] {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Main])

  override def write(kryo: Kryo, output: Output, t: Row): Unit = {
    if (t.size > 0) {
      t.schema.foreach { field: StructField => {
        output.writeString(field.name)
        field.dataType match {
          case BooleanType => output.writeBoolean(t.getAs[Boolean](field.name))
          case ByteType => output.writeByte(t.getAs[Byte](field.name))
          case ShortType => output.writeShort(t.getAs[Short](field.name))
          case IntegerType => output.writeInt(t.getAs[Int](field.name))
          case LongType => output.writeLong(t.getAs[Long](field.name))
          case FloatType => output.writeFloat(t.getAs[Float](field.name))
          case DoubleType => output.writeDouble(t.getAs[Double](field.name))
          case StringType => output.writeString(t.getAs[String](field.name))
          //case _ => kryo.writeClassAndObject(output, t.getAs(field.name))
        }
      }
      }
    }
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[Row]): Row = ???
}
