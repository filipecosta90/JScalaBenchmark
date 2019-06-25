package com.fcosta_oliveira

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.Row


class RowSerializer extends Serializer[org.apache.spark.sql.Row] {

  override def write(kryo: Kryo, output: Output, t: org.apache.spark.sql.Row): Unit = {
    t.schema.foreach(field => {
      output.writeString(field.name)
      kryo.writeClassAndObject(output, t.getAs(field.name))
    })
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[org.apache.spark.sql.Row]): Row = {
    //TODO: implement this
    val rowInstance = Row.fromSeq("TODO")
    rowInstance
  }
}
