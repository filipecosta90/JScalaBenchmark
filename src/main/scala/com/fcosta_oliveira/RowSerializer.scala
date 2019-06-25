package com.fcosta_oliveira

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}


class RowSerializer extends Serializer[org.apache.spark.sql.Row] {

  override def write(kryo: Kryo, output: Output, t: org.apache.spark.sql.Row): Unit = {
    t.schema.foreach(field => {
      output.writeString(field.name)
      field.dataType match {
        case BooleanType => output.writeBoolean(t.getAs[Boolean](field.name))
        case ByteType => output.writeByte(t.getAs[Byte](field.name))
        case ShortType =>output.writeShort(t.getAs[Short](field.name))
        case IntegerType => output.writeInt(t.getAs[Int](field.name))
        case LongType =>output.writeLong(t.getAs[Long](field.name))
        case FloatType => output.writeFloat(t.getAs[Float](field.name))
        case DoubleType => output.writeDouble(t.getAs[Double](field.name))
        case StringType => output.writeString(t.getAs[String](field.name))
        case _ => kryo.writeClassAndObject(output, t.getAs(field.name))
      }
    })
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[_ <: Row]): Row = {
    ???
  }

//  override def read(kryo: Kryo, input: Input, aClass: Class[org.apache.spark.sql.Row]): Row = {
//    //TODO: implement this
//    val rowInstance = Row.fromSeq("TODO")
//    rowInstance
//  }
}
