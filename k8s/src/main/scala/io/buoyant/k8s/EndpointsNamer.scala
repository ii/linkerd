package io.buoyant.k8s

import java.net.{InetAddress, InetSocketAddress}
import com.twitter.conversions.time._
import com.twitter.finagle.{Service => _, _}
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import io.buoyant.namer.Metadata
import scala.collection.{mutable, breakOut}
import scala.language.implicitConversions

class MultiNsNamer(
  idPrefix: Path,
  labelName: Option[String],
  mkApi: String => v1.NsApi,
  backoff: Stream[Duration] = EndpointsNamer.DefaultBackoff
)(implicit timer: Timer = DefaultTimer)
  extends EndpointsNamer(idPrefix, mkApi, labelName, backoff)(timer) {

  protected[this] override val variablePrefixLength: Int =
    MultiNsNamer.PrefixLen + labelName.size

  /**
   * Accepts names in the form:
   *   /<namespace>/<port-name>/<svc-name>[/<label-value>]/residual/path
   *
   * and attempts to bind an Addr by resolving named endpoint from the
   * kubernetes master.
   */
  def lookup(path: Path): Activity[NameTree[Name]] = {
    val lowercasePath = path.take(variablePrefixLength) match {
      case Path.Utf8(segments@_*) => Path.Utf8(segments.map(_.toLowerCase): _*)
    }
    (lowercasePath, labelName) match {
      case (id@Path.Utf8(nsName, portName, serviceName), None) =>
        val residual = path.drop(variablePrefixLength)
        log.debug("k8s lookup: %s %s", id.show, path.show)
        val portCache = PortMappingCache.fromService(mkApi(nsName).service(serviceName))
        lookupServices(nsName, portName, serviceName, portCache, id, residual)

      case (id@Path.Utf8(nsName, portName, serviceName, labelValue), Some(label)) =>
        val residual = path.drop(variablePrefixLength)
        log.debug("k8s lookup: %s %s %s", id.show, label, path.show)
        val labelSelector = Some(s"$label=$labelValue")
        val portCache = PortMappingCache.fromService(mkApi(nsName).service(serviceName))
        lookupServices(nsName, portName, serviceName, portCache, id, residual, labelSelector)

      case (id@Path.Utf8(nsName, portName, serviceName), Some(label)) =>
        log.debug("k8s lookup: ns %s service %s label value segment missing for label %s",
                  nsName, serviceName, portName, label)
        Activity.value(NameTree.Neg)

      case _ =>
        Activity.value(NameTree.Neg)
    }
  }
}

object MultiNsNamer {
  protected val PrefixLen = 3
}

class SingleNsNamer(
  idPrefix: Path,
  labelName: Option[String],
  nsName: String,
  mkApi: String => v1.NsApi,
  backoff: Stream[Duration] = EndpointsNamer.DefaultBackoff
)(implicit timer: Timer = DefaultTimer)
  extends EndpointsNamer(idPrefix, mkApi, labelName, backoff)(timer) {

  protected[this] override val variablePrefixLength: Int =
    SingleNsNamer.PrefixLen + labelName.size


  /**
   * Accepts names in the form:
   *   /<port-name>/<svc-name>[/<label-value>]/residual/path
   *
   * and attempts to bind an Addr by resolving named endpoint from the
   * kubernetes master.
   */
  def lookup(path: Path): Activity[NameTree[Name]] = {
    val lowercasePath = path.take(variablePrefixLength) match {
      case Path.Utf8(segments@_*) => Path.Utf8(segments.map(_.toLowerCase): _*)
    }
    (lowercasePath, labelName) match {
      case (id@Path.Utf8(portName, serviceName), None) =>
        val residual = path.drop(variablePrefixLength)
        log.debug("k8s lookup: %s %s", id.show, path.show)
        val portCache = PortMappingCache.fromService(mkApi(nsName).service(serviceName))
        lookupServices(nsName, portName, serviceName, portCache, id, residual)

      case (id@Path.Utf8(portName, serviceName, labelValue), Some(label)) =>
        val residual = path.drop(variablePrefixLength)
        log.debug("k8s lookup: %s %s %s", id.show, label, path.show)
        val labelSelector = Some(s"$label=$labelValue")
        val portCache = PortMappingCache.fromService(mkApi(nsName).service(serviceName))
        lookupServices(nsName, portName, serviceName, portCache, id, residual, labelSelector)

      case (id@Path.Utf8(portName, serviceName), Some(label)) =>
        log.debug("k8s lookup: ns %s service %s label value segment missing for label %s", nsName, serviceName, portName, label)
        Activity.value(NameTree.Neg)

      case _ =>
        Activity.value(NameTree.Neg)
    }
  }
}

object SingleNsNamer {
  protected val PrefixLen = 2
}

abstract class EndpointsNamer(
  idPrefix: Path,
  mkApi: String => v1.NsApi,
  labelName: Option[String] = None,
  backoff: Stream[Duration] = EndpointsNamer.DefaultBackoff
)(implicit timer: Timer = DefaultTimer)
  extends Namer {

  import EndpointsNamer._
  val cache = new EndpointsCache
  protected[this] val variablePrefixLength: Int

  private[k8s] def lookupServices(
    nsName: String,
    portName: String,
    serviceName: String,
    portCacheAct: Activity[PortMappingCache],
    id: Path,
    residual: Path,
    labelSelector: Option[String] = None
  ): Activity[NameTree[Name]] = {

    /**
     * Turn an `Option[Var[Addr]]` (as returned by `SvcCache.lookupNumberedPort`
     * or `SvcCache.port`) to an `Activity.State` containing a `NameTree`,
     * using the `id` and `residual` arguments passed to `lookupServices`.
     * @param lookup either `Some(Var[Addr])` if the lookup was successful,
     *                   or `None` if no service was found.
     * @return an `Activity.Ok` containing either a `NameTree.Leaf` with a
     *         `Name.Bound` to `addrs` if `lookup` is defined, or
     *         `NameTree.Neg` if `lookup` is empty
     */
    @inline
    def mkNameTree(lookup: Option[Var[Addr]]): Activity.State[NameTree[Name]] =
      Activity.Ok(lookup match {
        case Some(addrs) =>
          NameTree.Leaf(Name.Bound(addrs, idPrefix ++ id, residual))
        case None => NameTree.Neg
      })

    @inline def initCache(endpoints: v1.Endpoints): EndpointsCache = {
      cache.initialize(endpoints)
      cache
    }

    /**
     * Look up the service named `serviceName` from `endpoints`, and apply
     * function `f` to the result if it is defined.
     * @param f a function extracting a `Var[Option[Var[Addr]]]` from a
     *          `SvcCache`
     * @param endpoints the `EndpointsCache` to look up the service from
     * @return an `Activity` containing either the state of the service
     *         named by `serviceName` if one was found, or `NameTree.Neg`
     *         if it was undefined.
     */
    @inline
    def getService(f: SvcCache => Var[Option[Var[Addr]]])
                  (endpoints: EndpointsCache): Activity[NameTree[Name]] =
      // note: this would look nicer as a for-comprehension, but `for`
      //       doesn't like `Activity`, since it has `flatMap` but no
      //       `withFilter`.
      endpoints.services.flatMap { services =>
        services.get(serviceName).map { service =>
          // we found a `SvcCache` for `serviceName` – apply `f` to
          // it to extract the corresponding `Addr`, and make a
          // bound `NameTree` if it was defined.
          val state = f(service).map(mkNameTree)
          Activity(state)
        }.getOrElse {
          // TODO: log here
          Activity.value(NameTree.Neg)
        }
      }

    // make an `Activity` for the `EndpointsCache` named by `serviceName`
    // in `nsName`.
    // TODO: memoize?
    val endpointsAct: Activity[EndpointsCache] =
      mkApi(nsName)
        .endpoints(serviceName)
        .activity(initCache, labelSelector = labelSelector) { (_, event) =>
          // TODO: do `cache.update` and `cache.initialize` really need to be
          //       two separate functions?
          cache.update(event)
          cache
        }

    Try(portName.toInt).toOption match {
      case Some(portNumber) =>
        // if `portName` was successfully parsed as an `int`, then
        // we are dealing with a numbered port. we will thus also
        // need the port mappings cache, so join its activity with
        // the endpoints cache activity.

        // TODO: can we take the port mapping cache by value so we
        //       don't have to create it if it isn't needed?
        endpointsAct.join(portCacheAct)
          .flatMap { case ((endpoints, ports)) =>
            getService {
              _.lookupNumberedPort(ports, portNumber)
            }(endpoints)
          }
      case None =>
        // otherwise, we are dealing with a named port, so we can
        // just look up the service from the endpointsCache
        endpointsAct.flatMap {
          getService {
            _.port(portName)
          }
        }
    }
  }
}

object EndpointsNamer {
  val DefaultBackoff: Stream[Duration] =
    Backoff.exponentialJittered(10.milliseconds, 10.seconds)


  protected type PortMap = Map[String, Int]

  protected case class Endpoint(ip: InetAddress, nodeName: Option[String])

  protected object Endpoint {
    def apply(addr: v1.EndpointAddress): Endpoint =
      Endpoint(InetAddress.getByName(addr.ip), addr.nodeName)
  }

  protected case class Svc(endpoints: Set[Endpoint], ports: PortMap)

  protected[EndpointsNamer] case class SvcCache(name: String, init: Svc) {

    /**
     * For a given port number, apply the port mapping of the service.
     * The target port of the port mapping may be a named port and the
     * named port may or may not exist. The outer `Var[Option]` of the
     * return type tracks whether the port exists, and the inner
     * `Var[Addr]` tracks the actual  endpoints if the port does exist.
     */
    def lookupNumberedPort(mappings: PortMappingCache, portNumber: Int)
    : Var[Option[Var[Addr]]] =
      mappings(portNumber)
        .map { targetPort =>
          // target may be an int (port number) or string (port name)
          Try(targetPort.toInt).toOption match {
            case Some(targetPortNumber) =>
              // target port is a number and therefore exists
              Var(Some(port(targetPortNumber)))
            case None =>
              // target port is a name and may or may not exist
              port(targetPort)
          }
        }.getOrElse {
        Var(None)
      }

    private[this] val endpointsState = Var[Set[Endpoint]](init.endpoints)
    private[this] val portsState = Var[Map[String, Int]](init.ports)

    def port(portName: String): Var[Option[Var[Addr]]] = {
      portsState.map { portMap =>
        val portNumber = portMap.get(portName)
        portNumber.map(port)
      }
    }

    def port(portNumber: Int): Var[Addr] =
      endpointsState.map { endpoints =>
        val addrs: Set[Address] = endpoints.map { endpoint =>
          val isa = new InetSocketAddress(endpoint.ip, portNumber)
          Address.Inet(isa, endpoint.nodeName.map(Metadata.nodeName -> _).toMap)
        }
        Addr.Bound(addrs)
      }

    def ports: Var[Map[String, Int]] = portsState

    def update(subsets: Option[Seq[v1.EndpointSubset]]): Unit = {
      val (newEndpoints, newPorts) = subsets.toEndpointsAndPorts

      synchronized {
        val oldEndpoints = endpointsState.sample()
        if (newEndpoints != oldEndpoints) endpointsState() = newEndpoints
        val oldPorts = portsState.sample()
        if (newPorts != oldPorts) portsState() = newPorts
      }
    }
  }

  protected implicit class RichSubset(val subset: v1.EndpointSubset) extends AnyVal {
    def toPortMap: PortMap =
      (for {
        v1.EndpointPort(port, Some(name), maybeProto) <- subset.portsSeq
        if maybeProto.map(_.toUpperCase).getOrElse("TCP") == "TCP"
      } yield name -> port) (breakOut)

    def toEndpoints: Set[Endpoint] =
      for {address: v1.EndpointAddress <- subset.addressesSeq.toSet} yield {
        Endpoint(address)
      }
  }

  protected implicit class RichSubsetsSeq(val subsets: Option[Seq[v1.EndpointSubset]]) extends AnyVal {
    def toEndpointsAndPorts: (Set[Endpoint], PortMap) = {
      val result = for {
        subsetsSeq <- subsets.toSeq
        subset <- subsetsSeq
      } yield {
        (subset.toEndpoints, subset.toPortMap)
      }
      val (endpoints, ports) = result.unzip
      (endpoints.flatten.toSet, if (ports.isEmpty) Map.empty else ports.reduce(_ ++ _))
    }

    @inline def toSvc: Svc = Svc.tupled(toEndpointsAndPorts)
  }

  implicit def endpointsToSvcCache(endpoints: v1.Endpoints): Option[SvcCache] =
    endpoints.getName.map { name => SvcCache(name, endpoints.subsets.toSvc) }


  class EndpointsCache extends Ns.ObjectCache[v1.Endpoints, v1.EndpointsWatch] {

    private[this] val state = Var[Activity.State[Map[String, SvcCache]]](Activity.Pending)
    val services: Activity[Map[String, SvcCache]] = Activity(state)

    def initialize(endpoints: v1.Endpoints): Unit =
      add(endpoints)

    override def update(watch: v1.EndpointsWatch): Unit =
      watch match {
        case v1.EndpointsError(e) => log.error("k8s watch error: %s", e)
        case v1.EndpointsAdded(endpoints) => add(endpoints)
        case v1.EndpointsModified(endpoints) => add(endpoints)
        case v1.EndpointsDeleted(endpoints) => delete(endpoints)
      }

    private[this] def add(endpoints: v1.Endpoints): Unit =
      for {svc <- endpoints} synchronized {
        log.debug(s"k8s added: $svc")
        val svcs = state.sample() match {
          case Activity.Ok(svcs) => svcs
          case _ => Map.empty[String, SvcCache]
        }
        state() = Activity.Ok(svcs + (svc.name -> svc))
      }

    private[this] def modify(endpoints: v1.Endpoints): Unit =
      for {name <- endpoints.getName} synchronized {
        log.debug(s"k8s modified: $name")
        state.sample() match {
          case Activity.Ok(snap) =>
            snap.get(name) match {
              case None =>
                log.warning(s"k8s ns received modified watch for unknown service $name")
              case Some(svc) =>
                svc() = endpoints.subsets
            }
          case _ =>
        }
      }

    private[this] def delete(endpoints: v1.Endpoints): Unit =
      for {name <- endpoints.getName} synchronized {
        log.debug(s"k8s deleted: $name")
        state.sample() match {
          case Activity.Ok(snap) =>
            for (svc <- snap.get(name)) {
              state() = Activity.Ok(snap - name)
            }

          case _ =>
        }
      }
  }

}