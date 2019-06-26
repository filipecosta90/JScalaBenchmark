package com.fcosta_oliveira

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.openjdk.jmh.Main
import org.slf4j.{Logger, LoggerFactory}

class RowSerializer extends Serializer[Row] {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Main])

  // TODO: assess with Oleksiy (@fe2s) if the datatTypes are all covered
  override def write(kryo: Kryo, output: Output, t: Row): Unit = {
    // write the number of fields
    output.writeInt(t.size)
    if (t.size > 0) {
      t.schema.foreach { field: StructField => {
        output.writeString(field.name)
        kryo.writeClassAndObject(output, t.getAs(field.name))
        /*
                field.dataType match {
                  case BooleanType => { kryo.writeClassAndObject(, output, t.getAs[BooleanType](field.name)) }
                  case ByteType => output.writeByte(t.getAs[Byte](field.name))
                  case ShortType => output.writeShort(t.getAs[Short](field.name))
                  case IntegerType => output.writeInt(t.getAs[Int](field.name))
                  case LongType => output.writeLong(t.getAs[Long](field.name))
                  case FloatType => output.writeFloat(t.getAs[Float](field.name))
                  case DoubleType => output.writeDouble(t.getAs[Double](field.name))
                  case StringType => output.writeString(t.getAs[String](field.name))
                  //case _ => kryo.writeClassAndObject(output, t.getAs(field.name))
                }*/
      }
      }
    }
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[Row]): Row = {
    val size = input.readInt()
    val cols = new Array[Any](size)
    val structfields = new Array[StructField](size)
    // LOG.info("Reading #cols: " + size.toString)


    for (fieldnum <- 0 to size - 1) {
      val fieldName = input.readString()
      //LOG.info("Reading #fieldName: " + fieldName)
      var dataType: DataType = IntegerType
      val obj: AnyRef = kryo.readClassAndObject(input)

      if (obj.isInstanceOf[StringType]) {
        dataType = StringType
      }
      if (obj.isInstanceOf[BooleanType]) {
        dataType = BooleanType
      }

      if (obj.isInstanceOf[ByteType]) {
        dataType = ByteType
      }

      if (obj.isInstanceOf[ShortType]) {
        dataType = ShortType
      }

      if (obj.isInstanceOf[IntegerType]) {
        dataType = IntegerType
      }

      if (obj.isInstanceOf[LongType]) {
        dataType = LongType
      }

      if (obj.isInstanceOf[FloatType]) {
        dataType = FloatType
      }

      if (obj.isInstanceOf[DoubleType]) {
        dataType = DoubleType
      }

      structfields(fieldnum) = StructField(fieldName, dataType)
      cols(fieldnum) = obj
    }

    val schema = StructType(structfields)
    val newRow: Row = new GenericRowWithSchema(cols, schema)
    newRow
  }
}
