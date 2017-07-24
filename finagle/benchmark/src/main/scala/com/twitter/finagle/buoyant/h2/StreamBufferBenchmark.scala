package com.twitter.finagle.buoyant.h2

import com.twitter.concurrent.AsyncQueue
import com.twitter.io.Buf
import com.twitter.util.StdBenchAnnotations
import io.buoyant.test.Awaits
import org.openjdk.jmh.annotations._

// ./sbt 'project finagle-benchmark' 'jmh:run -i 20 -prof gc .*StreamBufferBenchmark.*'
@State(Scope.Benchmark)
class StreamBufferBenchmark extends StdBenchAnnotations with Awaits {

  val StreamLength = 10
  val Data = Buf.Utf8("Linkerd")

  @Param(Array("2", "3", "4", "5"))
  var fanout: Int = _

  @Benchmark
  def baseline(): Frame = {
    val q = new AsyncQueue[Frame]()
    val stream = Stream(q)
    var i = 0
    while (i < StreamLength - 1) {
      val frame = Frame.Data(Data, eos = false)
      q.offer(frame)
      await(stream.read())
    }
    val frame = Frame.Trailers()
    q.offer(frame)
    await(stream.read())
  }

  @Benchmark
  def buffered(): Frame = {
    val q = new AsyncQueue[Frame]()
    val stream = Stream(q)
    val buffered = new BufferedStream(stream)
    val child = buffered.fork().get
    var i = 0
    while (i < StreamLength - 1) {
      val frame = Frame.Data(Data, eos = false)
      q.offer(frame)
      await(child.read())
    }
    val frame = Frame.Trailers()
    q.offer(frame)
    await(child.read())
  }

  @Benchmark
  def bufferExceeded(): Frame = {
    val q = new AsyncQueue[Frame]()
    val stream = Stream(q)
    val buffered = new BufferedStream(stream, bufferCapacity = Data.length * StreamLength / 2)
    val child = buffered.fork().get
    var i = 0
    while (i < StreamLength - 1) {
      val frame = Frame.Data(Data, eos = false)
      q.offer(frame)
      await(child.read())
    }
    val frame = Frame.Trailers()
    q.offer(frame)
    await(child.read())
  }

  @Benchmark
  def forks(): Array[Frame] = {
    val q = new AsyncQueue[Frame]()
    val stream = Stream(q)
    val buffered = new BufferedStream(stream)
    val children = new Array[Stream](fanout)
    var j = 0
    while (j < fanout) {
      children(j) = buffered.fork().get
    }
    var i = 0
    while (i < StreamLength - 1) {
      val frame = Frame.Data(Data, eos = false)
      q.offer(frame)
      j = 0
      while (j < fanout) {
        await(children(j).read())
      }

    }
    val frame = Frame.Trailers()
    q.offer(frame)
    val finalFrames = new Array[Frame](fanout)
    j = 0
    while (j < fanout) {
      finalFrames(j) = await(children(j).read())
    }
    finalFrames
  }

  @Benchmark
  def forksBufferExceeded(): Array[Frame] = {
    val q = new AsyncQueue[Frame]()
    val stream = Stream(q)
    val buffered = new BufferedStream(stream, bufferCapacity = Data.length * StreamLength / 2)
    val children = new Array[Stream](fanout)
    var j = 0
    while (j < fanout) {
      children(j) = buffered.fork().get
    }
    var i = 0
    while (i < StreamLength - 1) {
      val frame = Frame.Data(Data, eos = false)
      q.offer(frame)
      j = 0
      while (j < fanout) {
        await(children(j).read())
      }

    }
    val frame = Frame.Trailers()
    q.offer(frame)
    val finalFrames = new Array[Frame](fanout)
    j = 0
    while (j < fanout) {
      await(children(j).read())
    }
    finalFrames
  }
}
