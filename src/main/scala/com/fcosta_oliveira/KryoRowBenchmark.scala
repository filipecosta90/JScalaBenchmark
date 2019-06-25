package com.fcosta_oliveira

import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.openjdk.jmh.Main
import org.openjdk.jmh.annotations._
import org.slf4j.{Logger, LoggerFactory}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement( time = 60 )
class KryoRowBenchmark {

  val kryo = new Kryo
  val records: Int = 100000
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Main])
  var seq: Seq[Row] = Seq(Row())
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
    kryo.register(classOf[org.apache.spark.sql.Row])
    kryo.addDefaultSerializer(classOf[org.apache.spark.sql.Row], classOf[RowSerializer])
    var recordsPos = 0;
    val colPos = 0
    data = StringUtils.repeat("x", colsize)
    LOG.info("started generating sequence data")
    for (recordsPos <- 1 to records) {
      var row : Row = Row()
      for (colPos <- 1 to ncols) {
        row.+(data)
      }
      seq ++= Seq(row)
    }
    LOG.info("ended generating sequence data")

    if (buffersize < ncols * colsize * blockSize) {
      LOG.warn("Setting buffersize to " + ( ncols * colsize * blockSize * 2 ) + " since " + buffersize + " was too small ")
      buffersize = ncols * colsize * blockSize * 2
    }

  }


  @Benchmark
  @OperationsPerInvocation(100000)
  def testDefaultSerializerSingleOutput(): Unit = {
    val output = new Output(buffersize)
    var blockcount : Int = 0
    seq.grouped(blockSize).foreach { row =>
      output.setPosition(0)
      // convert Seq to Array since we need a Serializable

      kryo.writeObject(output, row)
      //Flushes any buffered bytes and closes the underlying OutputStream, if any.
      output.close()
      blockcount+=1;
    }
  }

}
