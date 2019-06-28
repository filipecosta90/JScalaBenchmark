package com.fcosta_oliveira

import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.minlog.Log._
import com.esotericsoftware.kryo.io.{Input, Output, UnsafeInput, UnsafeOutput}
import com.esotericsoftware.minlog._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.openjdk.jmh.Main
import org.openjdk.jmh.annotations._
import org.slf4j.{Logger, LoggerFactory}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(time = 60)
class KryoRowBenchmark {
  // uncomment to set log level
  //Log.set(LEVEL_TRACE);

  val kryo = new Kryo

  val records: Int = 100000
  val seq: Array[Row] = new Array[Row](records)
  val recoveredSeq: Array[Row] = new Array[Row](records)
  var blocks: Array[Array[Byte]] = Array(Array())
  var blocksUnsafe: Array[Array[Byte]] = Array(Array())
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Main])
  // colsize
  @Param(Array("36"))
  var colsize: Int = _
  // ncols
  @Param(Array("400"))
  var ncols: Int = _
  // buffersize
  @Param(Array("4194304"))
  var buffersize: Int = _
  // blockSize
  @Param(Array("10"))
  var blockSize: Int = _
  private var data: String = null


  // The method is called once for each time for each full run of the benchmark.
  // A full run means a full "fork" including all warmup and benchmark iterations.
  @Setup(Level.Trial)
  def setup(): Unit = {

    val structfields = new Array[StructField](ncols)
    for (colPos <- 0 to ncols - 1) {
      structfields(colPos) = StructField("col" + colPos, StringType)
    }
    var schema = StructType(structfields)

    kryo.addDefaultSerializer(classOf[Row], new RowSerializer(schema))
    kryo.addDefaultSerializer(classOf[GenericRowWithSchema], new RowSerializer(schema))

    kryo.register(classOf[Row])
    kryo.register(classOf[GenericRow])
    kryo.register(classOf[GenericRowWithSchema])
    kryo.register(classOf[Array[Row]])
    kryo.register(classOf[Array[GenericRow]])
    kryo.getFieldSerializerConfig.isOptimizedGenerics

    if (buffersize < ncols * colsize * blockSize) {
      LOG.warn("Setting buffersize to " + (ncols * colsize * blockSize * 2) + " since " + buffersize + " was too small ")
      buffersize = ncols * colsize * blockSize * 2
    }

    val totalNumberBlocks = Integer.divideUnsigned(records, blockSize)
    LOG.info("Generating a total of " + totalNumberBlocks + " blocks, each with " + blockSize + " rows")

    blocks = new Array[Array[Byte]](totalNumberBlocks)
    blocksUnsafe = new Array[Array[Byte]](totalNumberBlocks)

    data = StringUtils.repeat("x", colsize)
    LOG.info("started generating sequence data")
    for (recordsPos <- 0 to records - 1) {
      val cols = new Array[Any](ncols)
      for (colPos <- 0 to ncols - 1) {
        cols(colPos) = data
      }
      val newRow: Row = new GenericRowWithSchema(cols, schema)

      seq(recordsPos) = newRow
    }
    LOG.info("ended generating sequence data")

    LOG.info("started generating SAFE blocks data")
    val output = new Output(buffersize)
    var blockNumber = 0
    seq.grouped(blockSize).foreach(arrayRow => {
      output.setPosition(0)
      arrayRow.foreach(row => {
        kryo.writeObject(output, row)

      })
      output.close()
      blocks(blockNumber) = output.toBytes
      blockNumber += 1
    }
    )
    LOG.info("ended generating SAFE blocks data")

    LOG.info("started generating UNSAFE blocks data")
    val unsafeOutput = new UnsafeOutput(buffersize)
    blockNumber = 0
    seq.grouped(blockSize).foreach(arrayRow => {
      unsafeOutput.setPosition(0)
      arrayRow.foreach(row => {
        kryo.writeObject(unsafeOutput, row)

      })
      unsafeOutput.close()
      blocksUnsafe(blockNumber) = unsafeOutput.toBytes
      blockNumber += 1
    }
    )
    LOG.info("ended generating UNSAFE blocks data")


  }


  @Benchmark
  @OperationsPerInvocation(100000)
  def testRowSerializer_Write_Safe(): Unit = {
    val output = new Output(buffersize)
    var blockNumber = 0
    seq.grouped(blockSize).foreach(arrayRow => {
      output.setPosition(0)
      arrayRow.foreach(row => {
        kryo.writeObject(output, row)
      })
      output.close()
      blocks(blockNumber) = output.toBytes
      blockNumber += 1
    }
    )
  }

  @Benchmark
  @OperationsPerInvocation(100000)
  def testRowSerializer_Write_Unsafe(): Unit = {
    val output = new UnsafeOutput(buffersize)
    var blockNumber = 0
    seq.grouped(blockSize).foreach(arrayRow => {
      output.setPosition(0)
      arrayRow.foreach(row => {
        kryo.writeObject(output, row)
      })
      output.close()
      blocks(blockNumber) = output.toBytes
      blockNumber += 1
    }
    )
  }


  @Benchmark
  @OperationsPerInvocation(100000)
  def testRowSerializer_Read_Safe(): Unit = {
    var rowNumber = 0
    blocks.grouped(blockSize).foreach(arrayRow => {
      arrayRow.foreach(bytes => {
        val input = new Input(bytes)
        val row = kryo.readObject(input, classOf[Row])
        recoveredSeq(rowNumber) = row
        //uncomment this to se recovered row
        //LOG.info(recoveredSeq(rowNumber).toString())
        rowNumber += 1
      }
      )
    }
    )
  }

  @Benchmark
  @OperationsPerInvocation(100000)
  def testRowSerializer_Read_UnSafe(): Unit = {
    var rowNumber = 0
    blocksUnsafe.grouped(blockSize).foreach(arrayRow => {
      arrayRow.foreach(bytes => {
        val input = new UnsafeInput(bytes)
        val row = kryo.readObject(input, classOf[Row])
        recoveredSeq(rowNumber) = row
        //uncomment this to se recovered row
        //LOG.info(recoveredSeq(rowNumber).toString())
        rowNumber += 1
      }
      )
    }
    )
  }

}
