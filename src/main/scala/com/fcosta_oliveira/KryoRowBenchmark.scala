package com.fcosta_oliveira

import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.objenesis.strategy.StdInstantiatorStrategy
import org.openjdk.jmh.Main
import org.openjdk.jmh.annotations._
import org.slf4j.{Logger, LoggerFactory}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(time = 60)
class KryoRowBenchmark {

  val kryo = new Kryo
  val records: Int = 100000
  val seq: Array[Row] = new Array[Row](records)
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Main])
  // colsize
  @Param(Array("36"))
  var colsize: Int = _
  // ncols
  @Param(Array("400"))
  var ncols: Int = _
  // buffersize
  @Param(Array("1"))
  var buffersize: Int = _
  // blockSize
  @Param(Array("1000"))
  var blockSize: Int = _
  private var data: String = null

  // Output for writing to a byte array.
  private var output = null

  // The method is called once for each time for each full run of the benchmark.
  // A full run means a full "fork" including all warmup and benchmark iterations.
  @Setup(Level.Trial)
  def setup(): Unit = {
    kryo.addDefaultSerializer(classOf[Row], classOf[RowSerializer])
    kryo.register(classOf[Row])
    kryo.register(classOf[GenericRow])

    kryo.getInstantiatorStrategy.asInstanceOf[Kryo.DefaultInstantiatorStrategy].setFallbackInstantiatorStrategy(new StdInstantiatorStrategy)


    data = StringUtils.repeat("x", colsize)
    LOG.info("started generating sequence data")
    for (recordsPos <- 0 to records - 1) {
      val cols = new Array[Any](ncols)
      val structfields = new Array[StructField](ncols)
      for (colPos <- 0 to ncols - 1) {
        cols(colPos) = data
        structfields(colPos) = StructField("col" + colPos, StringType)
      }
      var schema = StructType(structfields)
      val newRow: Row = new GenericRowWithSchema(cols, schema)

      seq(recordsPos) = newRow
    }
    LOG.info("ended generating sequence data")

    if (buffersize < ncols * colsize * blockSize) {
      LOG.warn("Setting buffersize to " + (ncols * colsize * blockSize * 2) + " since " + buffersize + " was too small ")
      buffersize = ncols * colsize * blockSize * 2
    }

  }


  @Benchmark
  @OperationsPerInvocation(100000)
  def testDefaultSerializerSingleOutput(): Unit = {
    val output = new Output(buffersize)
    var blockcount: Int = 0
    seq.grouped(blockSize).foreach(arrayRow => {
      output.setPosition(0)
      arrayRow.foreach(row => {
        kryo.writeObject(output, row)
      })
      output.close()
      blockcount += 1;
    }
    )
  }

}
