package io.buoyant.k8s

import com.twitter.conversions.time._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import io.buoyant.k8s.Ns.ListCache

abstract class Ns[O <: KubeObject : Manifest, W <: Watch[O] : Manifest, L <: KubeList[O] : Manifest, Cache <: ListCache[O, W, L]](
  backoff: Stream[Duration] = Backoff.exponentialJittered(10.milliseconds, 10.seconds),
  timer: Timer = DefaultTimer
) {
  // note that caches must be updated with synchronized
  private[this] val caches = Var[Map[String, Cache]](Map.empty[String, Cache])
  // XXX once a namespace is watched, it is watched forever.
  private[this] var _watches = Map.empty[String, Activity[Closable]]

  /**
   * Returns an Activity backed by a Future.  The resultant Activity is pending until the
   * original future is satisfied.  When the Future is successful, the Activity becomes
   * an Activity.Ok with a fixed value from the Future.  If the Future fails, the Activity
   * becomes an Activity.Failed and the Future is retried with the given backoff schedule.
   * Therefore, the legal state transitions are:
   *
   * Pending -> Ok
   * Pending -> Failed
   * Failed -> Failed
   * Failed -> Ok
   */
  private[this] def retryToActivity[T](go: => Future[T]): Activity[T] = {
    val state = Var[Activity.State[T]](Activity.Pending)
    _retryToActivity(backoff, state)(go)
    Activity(state)
  }

  private[this] def _retryToActivity[T](
    remainingBackoff: Stream[Duration],
    state: Var[Activity.State[T]] with Updatable[Activity.State[T]] = Var[Activity.State[T]](Activity.Pending)
  )(go: => Future[T]): Unit = {
    val _ = go.respond {
      case Return(t) =>
        state() = Activity.Ok(t)
      case Throw(e) =>
        state() = Activity.Failed(e)
        remainingBackoff match {
          case delay #:: rest =>
            val _ = Future.sleep(delay)(timer).onSuccess { _ => _retryToActivity(rest, state)(go) }
          case Stream.Empty =>
        }
    }
  }

  protected def mkResource(name: String): NsListResource[O, W, L]

  protected def mkCache(name: String): Cache

  def get(name: String, labelSelector: Option[String]): Cache = synchronized {
    caches.sample.get(name) match {
      case Some(ns) => ns
      case None =>
        val ns = mkCache(name)
        val resource = mkResource(name)
        val closable = retryToActivity { watch(resource, labelSelector, ns) }
        _watches += (name -> closable)
        caches() = caches.sample + (name -> ns)
        ns
    }
  }

  val namespaces: Var[Set[String]] = caches.map(_.keySet)

  private[this] def watch(
    resource: NsListResource[O, W, L],
    labelSelector: Option[String],
    cache: Cache
  ): Future[Closable] = {

    Trace.letClear {
      log.info("k8s initializing %s", resource.ns)
      resource.get().map { list =>
        cache.initialize(list)
        val (updates, closable) = resource.watch(
          labelSelector = labelSelector,
          resourceVersion = list.metadata.flatMap(_.resourceVersion)
        )
        // fire-and-forget this traversal over an AsyncStream that updates the services state
        val _ = updates.foreach(cache.update)
        closable
      }.onFailure { e =>
        log.error(e, "k8s failed to list endpoints")
      }
    }
  }
}

object Ns {

  trait CacheLike[O <: KubeObject, W <: Watch[O], I] {
    protected[CacheLike] type VarUp[T] = Var[T] with Updatable[T]

    /**
     * We can stabilize this by changing the type to Var[Option[Var[T]]].
     * If this Option changes from None to Some or vice versa, the outer Var will
     * update.  If the value contained in the Some changes, only the inner Var
     * will update.
     */
    protected[ObjectCache] def stabilize[T](unstable: Var[Option[T]]): Var[Option[Var[T]]] = {
      val init = unstable.sample().map(Var(_))
      Var.async[Option[VarUp[T]]](init) { update =>
        // the current inner Var, null if the outer Var is None
        @volatile var current: VarUp[T] = null

        unstable.changes.respond {
          case Some(t) if current == null =>
            // T created
            current = Var(t)
            update() = Some(current)
          case Some(t) =>
            // T modified
            current() = t
          case None =>
            // T deleted
            current = null
            update() = None
        }
      }
    }
    def update(event: W): Unit

    def initialize(init: I): Unit
  }

  abstract class ObjectCache[O <: KubeObject : Manifest, W <: Watch[O] : Manifest]
    extends CacheLike[O, W, O]

  abstract class ListCache[O <: KubeObject : Manifest, W <: Watch[O] : Manifest, L <: KubeList[O] : Manifest]
    extends CacheLike[O, W, L]
}
