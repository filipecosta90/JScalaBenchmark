package com.fcosta_oliveira

import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.lang3.StringUtils
import org.openjdk.jmh.Main
import org.openjdk.jmh.annotations._
import org.slf4j.{Logger, LoggerFactory}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement( time = 60 )
class KryoBenchmark {

  val kryo = new Kryo
  val records: Int = 100000
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Main])
  var seq: Seq[Array[Any]] = Seq(Array())
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
    kryo.register(classOf[Array[Array[Any]]])
    kryo.register(classOf[Seq[Array[Any]]])
    kryo.register(classOf[Array[Any]])
    kryo.register(classOf[Seq[Any]])

    var recordsPos = 0;
    val colPos = 0
    data = StringUtils.repeat("x", colsize)
    LOG.info("started generating sequence data")
    for (recordsPos <- 1 to records) {
      var recordArraY = new Array[Any](ncols * 2)
      for (colPos <- 1 to ncols) {
        recordArraY(colPos * 2 - 2) = colPos
        recordArraY(colPos * 2 - 1) = data
      }
      seq ++= Seq(recordArraY)
    }
    LOG.info("ended generating sequence data")

    if (buffersize < ncols * colsize * blockSize) {
      LOG.warn("Setting buffersize to " + ( ncols * colsize * blockSize * 2 ) + " since " + buffersize + " was too small ")
      buffersize = ncols * colsize * blockSize * 2
    }

  }


  @Benchmark
  @OperationsPerInvocation(100000)
  def testMethod(): Unit = {
    val output = new Output(buffersize)
    var recordnum = 0
    var blockcount : Int = 0
    seq.grouped(blockSize).foreach { p =>
      output.setPosition(0)
      // convert Seq to Array since we need a Serializable
      val block = p.toArray
      kryo.writeObject(output, block)
      //Flushes any buffered bytes and closes the underlying OutputStream, if any.
      output.close()
      blockcount+=1;
    }
    LOG.info("serialized " + blockcount + " blocks, totalling " + output.total() + " bytes")
  }

}
