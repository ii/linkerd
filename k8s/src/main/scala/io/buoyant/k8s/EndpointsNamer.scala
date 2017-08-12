package io.buoyant.k8s

import java.net.{InetAddress, InetSocketAddress}
import com.twitter.conversions.time._
import com.twitter.finagle.{Service => _, _}
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import io.buoyant.k8s.EndpointsNamer.EndpointsCache
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
        log.debug("k8s lookup: ns %s service %s label value segment missing for label %s", nsName, serviceName, portName, label)
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


  //  private[k8s] val serviceNs = new Ns[
  //    v1.Service,
  //    v1.ServiceWatch,
  //    v1.ServiceList,
  //    PortCache
  //    ](backoff, timer) {
  //    override protected def mkResource(name: String) = mkApi(name).services
  //
  //    override protected def mkCache(name: String) = new PortCache()
  //  }

  @inline
  private[this] def mkNameTree(id: Path, residual: Path)
    (maybeAddrs: Option[Var[Addr]]): Activity.State[NameTree[Name]] =
    maybeAddrs match {
      case Some(addrs) => Activity.Ok(NameTree.Leaf(Name.Bound(addrs, idPrefix ++ id, residual)))
      case None => Activity.Ok(NameTree.Neg)
    }


  private[k8s] def lookupServices(
    nsName: String,
    portName: String,
    serviceName: String,
    portCacheAct: Activity[PortMappingCache],
    id: Path,
    residual: Path,
    labelSelector: Option[String] = None
  ): Activity[NameTree[Name]] = {

    val nameTree = mkNameTree(id, residual) _

    @inline def initCache(endpoints: v1.Endpoints): EndpointsCache = {
      // wish i didn't have to call initialize then get here, just to get the addrs
      // from the Endpoints object. who wrote this api anyway? /me raises hand. whoops.
      cache.initialize(endpoints)
      cache
    }

    @inline def getService(endpoints: EndpointsCache)(f: SvcCache => Var[Option[Var[Addr]]]) =
      endpoints.services.flatMap { services =>
        services.get(serviceName).map { service =>
          val lookup = f(service)
            .map(nameTree)
          Activity(lookup)
        }.getOrElse(Activity.value(NameTree.Neg))
      }

    val endpointsAct: Activity[EndpointsCache] =
      mkApi(nsName)
        .endpoints(serviceName)
        .activity(initCache, labelSelector = labelSelector) { (_, event) =>
          // and similarly update then get is not great programming either
          cache.update(event)
          cache
        }

    Try(portName.toInt).toOption match {
      case Some(portNumber) =>
        endpointsAct.join(portCacheAct)
          .flatMap { case ((endpoints, ports)) =>
            getService(endpoints) { service =>
              service.lookupNumberedPort(ports, portNumber)
            }
          }
      case None =>
        endpointsAct.flatMap { endpoints =>
          getService(endpoints) { service =>
            service.port(portName)
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
     * For a given port number, apply the port mapping of the service.  The target port of the port
     * mapping may be a named port and the named port may or may not exist.  The outer Var[Option]
     * of the return type tracks whether the port exists and the inner Var[Addr] tracks the actual
     * endpoints if the port does exist.
     */
    def lookupNumberedPort(
      mappings: PortMappingCache,
      portNumber: Int
    ): Var[Option[Var[Addr]]] =
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
      (endpoints.flatten.toSet, ports.reduce(_ ++ _))
    }

    @inline def toSvc: Svc = Svc.tupled(toEndpointsAndPorts)
  }

  implicit def endpointsToSvcCache(endpoints: v1.Endpoints): Option[SvcCache] =
    endpoints.getName.map { name => SvcCache(name, endpoints.subsets.toSvc) }


  class EndpointsCache extends Ns.ObjectCache[v1.Endpoints, v1.EndpointsWatch] {

    import EndpointsCache._

    private[this] val state = Var[Activity.State[Map[String, SvcCache]]](Activity.Pending)
    private[this] var cache = Map.empty[CacheKey, VarUp[Addr]]

    val services: Activity[Map[String, SvcCache]] = Activity(state)

    def getNumberedPort(nsName: String, serviceName: String, portNumber: Int): Option[VarUp[Addr]]
    = ???

    def initialize(endpoints: v1.Endpoints): Unit =
      add(endpoints)

    override def update(watch: v1.EndpointsWatch): Unit =
      watch match {
        case v1.EndpointsError(e) => log.error("k8s watch error: %s", e)
        case v1.EndpointsAdded(endpoints) => add(endpoints)
        case v1.EndpointsModified(endpoints) => add(endpoints)
        case v1.EndpointsDeleted(endpoints) => delete(endpoints)
      }

    def get(nsName: String, portName: String, serviceName: String): Option[Var[Addr]] =
      cache.get(CacheKey(nsName, portName, serviceName))

    private[this] def add(endpoints: v1.Endpoints): Unit = {
      val (endpointsMap, portsMap) = toMap(endpoints)
      for {(key, addr) <- endpointsMap} synchronized {
        log.debug("k8s ns %s added service: %s; port: %s", key.nsName, key.serviceName, key.portName)
        cache += key -> Var(addr)
      }
      for {svc: SvcCache <- endpoints} synchronized {
        val svcs = state.sample() match {
          case Activity.Ok(svcs) => svcs
          case _ => Map.empty[String, SvcCache]
        }
        state() = Activity.Ok(svcs + (svc.name -> svc))
      }
    }


    private[this] def modify(endpoints: v1.Endpoints): Unit = {
      val (endpointsMap, portsMap) = toMap(endpoints)
      for {(key, modified) <- endpointsMap} synchronized {
        cache.get(key) match {
          case Some(addr) =>
            log.info("k8s ns %s modified service: %s; port %s", key.nsName, key.serviceName, key.portName)
            addr.update(modified)
          case None =>
            log.info(
              s"k8s ns ${key.nsName} added service ${key.serviceName}; port ${key.portName}")
            cache += key -> Var(modified)
        }
      }
    }

    private[this] def delete(endpoints: v1.Endpoints): Unit = {
      val (endpointsMap, portsMap) = toMap(endpoints)
      for {(key, _) <- endpointsMap} synchronized {
        cache.get(key) match {
          case Some(addr) => addr.update(Addr.Neg)
          case None =>
            log.warning(
              "k8s ns %s received delete watch for unknown service %s; port %s",
              key.nsName, key.serviceName, key.portName
            )
        }
      }
    }
  }

  object EndpointsCache {

    private case class PortAddr(portName: String, addr: Address)

    private case class CacheKey(nsName: String, portName: String, serviceName: String)

    /**
     * Convert a [[v1.Endpoints]] object to a map of `(namespace, port, service) -> Address`
     *
     * @param endpoints
     * @return
     */
    private def toMap(endpoints: v1.Endpoints): (Map[CacheKey, Addr], Map[Int, String]) = {
      val endpointMap = mutable.Map.empty[CacheKey, Addr]
      val portNamesMap = mutable.Map.empty[Int, String]
      for {
        metadata <- endpoints.metadata
        namespace <- metadata.namespace
        name <- metadata.name
      } {
        val portsAndAddrs = for {
          subset <- endpoints.subsetsSeq
          address <- subset.addressesSeq
          port <- subset.portsSeq
          portName <- port.name
          portNum = port.port
        } yield {
          portNamesMap(portNum) = portName
          PortAddr(portName, Address(address.ip, portNum))
        }
        portsAndAddrs
          .groupBy(_.portName)
          .mapValues {
            _.map(_.addr)
          }
          .foreach { case (portName, addresses) =>
            val key = CacheKey(namespace, portName, name)
            endpointMap(key) = Addr.Bound(addresses: _*)
          }
      }
      (endpointMap.toMap, portNamesMap.toMap)
    }
  }

}