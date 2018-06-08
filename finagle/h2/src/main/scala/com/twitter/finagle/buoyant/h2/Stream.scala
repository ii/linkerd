package com.twitter.finagle.buoyant.h2

import com.twitter.concurrent.AsyncQueue
import com.twitter.io.Buf
import com.twitter.util.{Future, Promise, Return, Throw, Try}
import io.netty.buffer.{ByteBuf, Unpooled}
import java.nio.charset.{Charset, StandardCharsets => JChar}

/**
 * A Stream represents a stream of Data frames, optionally
 * followed by Trailers.
 *
 * A Stream is not prescriptive as to how data is produced. However,
 * flow control semantics are built into the Stream--consumers MUST
 * release each data frame after it has processed its contents.
 *
 * Consumers SHOULD call `read()` until it fails (i.e. when the
 * stream is fully closed).
 *
 * If a consumer cancels a `read()` Future, the stream is reset.
 */
trait Stream {
  override def toString = s"Stream(isEmpty=$isEmpty, onEnd=${onEnd.poll})"

  def isEmpty: Boolean
  final def nonEmpty: Boolean = !isEmpty

  def read(): Future[Frame]

  /**
   * Satisfied when an end-of-stream frame has been read from this
   * stream.
   *
   * If the stream is reset prematurely, onEnd fails with a [[Reset]].
   */
  def onEnd: Future[Unit]

  /**
   * Wraps this [[Stream]] with a [[StreamOnFrame]] that calls the
   * provided function on each frame after [[Stream.read read()]]
   *
   * @param onFrame the function to call on each frame.
   * @return a [[StreamOnFrame]] [[StreamProxy]] wrapping this [[Stream]]
   */
  def onFrame(onFrame: Try[Frame] => Unit): Stream = new StreamOnFrame(this, onFrame)

  /**
   * Wraps this [[Stream]]  with a function `f` that is called on each frame in
   * the stream. The sequence of [[Frame]]s yielded by `f` will be inserted into
   * the stream in order at the current position.
   *
   * @note that in order to avoid violating flow control, `f` must either take
   *       ownership over the frame and release it, or return it in the returned
   *       sequence of frames.
   * @see [[StreamFlatMap]]
   * @param f the function called on each [[Stream]]
   * @return a [[StreamFlatMap]] [[StreamProxy]] wrapping this [[Stream]]
   */
  def flatMap(f: Frame => Seq[Frame]): Stream = new StreamFlatMap(this, f)

}

/**
 * A Stream of Frames
 */
object Stream {

  // TODO Create a dedicated Static stream type that indicates the
  // entire message is buffered (i.e. so that retries may be
  // performed).  This would require some form of buffering, ideally
  // in the dispatcher and server stream

  /**
   * Fail the queue and release all of its Frames.  This can be called as a safe alternative to
   * q.fail(e, discard = true).
   */
  def failAndDrainFrameQueue(q: AsyncQueue[Frame], e: Throwable): Unit = {
    q.fail(e, discard = false)
    def drain(q: AsyncQueue[Frame]): Future[Nothing] = {
      q.poll().flatMap { f =>
        f.release()
        drain(q)
      }
    }
    drain(q)
    ()
  }

  /**
   * In order to create a stream, we need a mechanism to write to it.
   */
  trait Writer {

    /**
     * Write an object to a Stream so that it may be read as a Frame
     * (i.e. onto an underlying transport). The returned future is not
     * satisfied until the frame is written and released.
     */
    def write(frame: Frame): Future[Unit]

    def reset(err: Reset): Unit
    def close(): Unit
  }

  private[h2] trait AsyncQueueReader extends Stream {
    protected[this] val frameQ: AsyncQueue[Frame]

    override def isEmpty = false

    private[this] val endP = new Promise[Unit]
    override def onEnd: Future[Unit] = endP

    private[this] val endOnReleaseIfEnd: Try[Frame] => Unit = {
      case Return(f) => if (f.isEnd) endP.become(f.onRelease)
      case Throw(e) => endP.updateIfEmpty(Throw(e)); ()
    }

    override def read(): Future[Frame] = {
      val f = frameQ.poll()
      f.respond(endOnReleaseIfEnd)
      failOnInterrupt(f, frameQ)
    }
  }

  private[this] def failOnInterrupt[T](f: Future[T], q: AsyncQueue[Frame]): Future[T] = {
    val p = new Promise[T] with Promise.InterruptHandler {
      override protected def onInterrupt(e: Throwable): Unit = {
        failAndDrainFrameQueue(q, e)
        f.raise(e)
      }
    }
    f.proxyTo(p)
    p
  }

  private class AsyncQueueReaderWriter extends AsyncQueueReader with Writer {
    override protected[this] val frameQ = new AsyncQueue[Frame]

    override def write(f: Frame): Future[Unit] =
      if (frameQ.offer(f)) failOnInterrupt(f.onRelease, frameQ)
      else Future.exception(Reset.Closed)

    override def reset(err: Reset): Unit = {
      failAndDrainFrameQueue(frameQ, err)
    }
    override def close(): Unit = frameQ.fail(Reset.NoError, discard = false)
  }

  def apply(q: AsyncQueue[Frame]): Stream =
    new AsyncQueueReader { override protected[this] val frameQ = q }

  def apply(): Stream with Writer =
    new AsyncQueueReaderWriter

  def const(f: Frame): Stream = {
    val q = new AsyncQueue[Frame]
    q.offer(f)
    apply(q)
  }

  def const(buf: ByteBuf): Stream =
    const(Frame.Data.eos(buf))

  def const(s: String): Stream = {
    val bytes = s.getBytes(JChar.UTF_8)
    const(Unpooled.wrappedBuffer(bytes))
  }

  def empty(): Stream with Writer = empty(new AsyncQueue[Frame](1))

  def empty(frameQ: AsyncQueue[Frame]): Stream with Writer =
    new Stream with Writer {
      override def isEmpty = true
      override def onEnd = Future.Unit
      override def read(): Future[Frame] = failOnInterrupt(frameQ.poll(), frameQ)
      override def write(f: Frame): Future[Unit] = {
        failAndDrainFrameQueue(frameQ, Reset.Closed)
        Future.exception(Reset.Closed)
      }
      override def reset(err: Reset): Unit = {
        frameQ.fail(err, discard = false)
        failAndDrainFrameQueue(frameQ, err)
      }
      override def close(): Unit = frameQ.fail(Reset.NoError, discard = false)
    }

}

/**
 * A single item in a Stream.
 */
sealed trait Frame {

  /**
   * When `isEnd` is true, no further events will be returned on the
   * Stream.
   */
  def isEnd: Boolean

  def onRelease: Future[Unit]
  def release(): Future[Unit]
}

object Frame {
  /**
   * A frame containing aribtrary data.
   *
   * `release()` MUST be called so that the producer may manage flow control.
   */
  trait Data extends Frame {
    override def toString = s"Frame.Data(length=${buf.readableBytes()}, isEnd=$isEnd)"
    def buf: ByteBuf
  }

  object Data {

    def apply(buf0: ByteBuf, eos: Boolean, release0: () => Future[Unit]): Data = new Data {
      def buf = buf0
      private[this] val releaseP = new Promise[Unit]
      def onRelease = releaseP
      def release() = {
        val f = release0()
        releaseP.become(f)
        f
      }
      def isEnd = eos
    }

    object NoopRelease extends Function0[Future[Unit]] {
      override def apply(): Future[Unit] = Future.Unit
    }

    def apply(buf: ByteBuf, eos: Boolean): Data =
      apply(buf, eos, NoopRelease)

    def apply(s: String, eos: Boolean, release: () => Future[Unit]): Data = {
      val bytes = s.getBytes(JChar.UTF_8)
      apply(Unpooled.wrappedBuffer(bytes), eos, release)
    }

    def apply(s: String, eos: Boolean): Data = {
      val bytes = s.getBytes(JChar.UTF_8)
      apply(Unpooled.wrappedBuffer(bytes), eos)
    }

    def eos(buf: ByteBuf): Data = apply(buf, true)
    def eos(s: String): Data = apply(s, true)

    def copy(frame: Data, eos: Boolean): Data = new Data {
      def buf = frame.buf
      def onRelease = frame.onRelease
      def release() = frame.release()
      def isEnd = eos
    }
  }

  /** A terminal Frame including headers. */
  trait Trailers extends Frame with Headers { headers =>
    override def toString = s"Frame.Trailers(${headers.toSeq})"
    final override def isEnd = true
  }

  object Trailers {

    private class Impl(orig: Seq[(String, String)])
      extends Headers.Impl(orig) with Trailers {

      private[this] val promise = new Promise[Unit]
      def release(): Future[Unit] = {
        promise.updateIfEmpty(Return.Unit)
        Future.Unit
      }
      def onRelease: Future[Unit] = promise
    }

    def apply(pairs: Seq[(String, String)]): Trailers =
      new Impl(pairs)

    def apply(hd: (String, String), tl: (String, String)*): Trailers =
      apply(hd +: tl)

    def apply(): Trailers =
      apply(Nil)
  }

}
