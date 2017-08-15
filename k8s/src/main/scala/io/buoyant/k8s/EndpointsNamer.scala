package io.buoyant.k8s

import java.net.{InetAddress, InetSocketAddress}
import com.twitter.conversions.time._
import com.twitter.finagle.{Service => _, _}
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import io.buoyant.namer.Metadata
import EndpointsNamer._
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
        lookupServices(nsName, portName, serviceName, id, residual)

      case (id@Path.Utf8(nsName, portName, serviceName, labelValue), Some(label)) =>
        val residual = path.drop(variablePrefixLength)
        log.debug("k8s lookup: %s %s %s", id.show, label, path.show)
        val labelSelector = Some(s"$label=$labelValue")
        lookupServices(nsName, portName, serviceName, id, residual, labelSelector)

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
        lookupServices(nsName, portName, serviceName, id, residual)

      case (id@Path.Utf8(portName, serviceName, labelValue), Some(label)) =>
        val residual = path.drop(variablePrefixLength)
        log.debug("k8s lookup: %s %s %s", id.show, label, path.show)
        val labelSelector = Some(s"$label=$labelValue")
        lookupServices(nsName, portName, serviceName, id, residual, labelSelector)

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
  extends Namer with Stabilize {

  import EndpointsNamer._
  protected[this] val variablePrefixLength: Int

  // memoize port remapping watch activities so that we don't have to
  // create multiple watches on the same `Services` API object.
  private[this] val portRemappings =
    mutable.HashMap[(String, String), Activity[NumberedPortMap]]()

  /**
   * Watch the numbered-port remappings for the service named `serviceName`
   * in the namespace named `nsName`.
   * @param nsName the name of the Kubernetes namespace.
   * @param serviceName the name of the Kubernetes service.
   * @return an `Activity` containing a `Map[Int, String]` representing the
   *         port number to port name mappings
   * @note that the corresponding `Activity` instances are cached so we don't
   *       create multiple watches on the same Kubernetes objects – meaning,
   *       if you look up the port remappings in a given (nsName, serviceName)
   *       pair multiple times, you will always get back the same `Activity`,
   *       which is created the first time that pair is looked up.
   */
  private[k8s] def numberedPortRemappings(
    nsName: String,
    serviceName: String,
    labelSelector: Option[String]
  ) : Activity[NumberedPortMap] = {
    def _getPortMap() =
      mkApi(nsName)
        .service(serviceName)
        .activity(_.portMappings, labelSelector = labelSelector) {
          case ((oldMap, v1.ServiceAdded(service))) =>
            val newMap = service.portMappings
            newMap.foreach {
              case ((port, name)) if oldMap.contains(port) =>
                log.debug(s"k8s ns $nsName service $serviceName remapped port $port -> $name")
              case ((port, name)) =>
                log.debug(s"k8s ns $nsName service $serviceName added port mapping $port -> $name")
              }
            oldMap ++ newMap
          case ((oldMap, v1.ServiceModified(service))) =>
            val newMap = service.portMappings
            newMap.foreach {
              case ((port, name)) if oldMap.contains(port) =>
                log.debug(s"k8s ns $nsName service $serviceName remapped port $port -> $name")
              case ((port, name)) =>
                log.debug(s"k8s ns $nsName service $serviceName added port mapping $port -> $name")
            }
            oldMap ++ newMap
          case ((oldMap, v1.ServiceDeleted(service))) =>
            val newMap = service.portMappings
              // log deleted ports
            for { deletedPort <- oldMap.keySet &~ newMap.keySet }
              log.debug(s"k8s ns $nsName service $serviceName deleted port mapping for $deletedPort")
            newMap
          case ((oldMap, v1.ServiceError(error))) =>
            log.error(s"k8s ns $nsName service $serviceName watch error $error")
            oldMap
        }
    portRemappings.getOrElseUpdate((nsName, serviceName), _getPortMap())
  }

  private[k8s] def endpointsCache(
    nsName: String,
    serviceName: String,
    labelSelector: Option[String]
  ): Activity[EndpointsCache] =
    mkApi(nsName)
      .endpoints(serviceName)
      .activity(_.cache, labelSelector = labelSelector){ case ((cache, event)) =>
        event match {
          case v1.EndpointsAdded(endpoints) => cache.update(endpoints)
          case v1.EndpointsModified(endpoints) => cache.update(endpoints)
          case v1.EndpointsDeleted(endpoints) => cache.update(endpoints)
          case v1.EndpointsError(error) =>
            log.debug(s"k8s ns $nsName service $serviceName endpoints watch error $error")
        }
        cache
      }


  @inline
  private[this] def mkNameTree(
    id: Path,
    residual: Path
  )(lookup: Option[Var[Set[Address]]]): Activity.State[NameTree[Name]] =
    Activity.Ok(
      lookup.map { addresses =>
        val addrs = addresses.map { Addr.Bound(_) }
        NameTree.Leaf(Name.Bound(addrs, idPrefix ++ id, residual))
      }
      .getOrElse { NameTree.Neg }
    )

  private[k8s] def lookupServices(
    nsName: String,
    portName: String,
    serviceName: String,
    id: Path,
    residual: Path,
    labelSelector: Option[String] = None
  ): Activity[NameTree[Name]] = {
    val cache = endpointsCache(nsName, serviceName, labelSelector)
    Try(portName.toInt).toOption match {
      case Some(portNumber) =>
        // if `portName` was successfully parsed as an `int`, then
        // we are dealing with a numbered port. we will thus also
        // need the port mappings from the `Service` API response,
        // so join its activity with the endpoints cache activity.
        cache.join(numberedPortRemappings(nsName, serviceName, labelSelector))
          .flatMap { case ((endpoints, ports)) =>
            val state =
              endpoints.lookupNumberedPort(ports, portNumber)
                .map(mkNameTree(id, residual))
            Activity(state)
          }
      case None =>
        // otherwise, we are dealing with a named port, so we can
        // just look up the service from the endpointsCache
        cache.flatMap { endpoints =>
          val state =
            endpoints.lookupNamedPort(portName).map(mkNameTree(id, residual))
          Activity(state)
        }
    }
  }
}

object EndpointsNamer {
  val DefaultBackoff: Stream[Duration] =
    Backoff.exponentialJittered(10.milliseconds, 10.seconds)


  protected type PortMap = Map[String, Int]
  protected type NumberedPortMap = Map[Int, String]

  protected case class Endpoint(ip: InetAddress, nodeName: Option[String])

  protected object Endpoint {
    def apply(addr: v1.EndpointAddress): Endpoint =
      Endpoint(InetAddress.getByName(addr.ip), addr.nodeName)
  }

  protected[EndpointsNamer] case class EndpointsCache(endpoints: Set[Endpoint], ports: PortMap) extends Stabilize {
    /**
     * For a given port number, apply the port mapping of the service.
     * The target port of the port mapping may be a named port and the
     * named port may or may not exist. The outer `Var[Option]` of the
     * return type tracks whether the port exists, and the inner
     * `Var[Addr]` tracks the actual  endpoints if the port does exist.
     */
    def lookupNumberedPort(mappings: NumberedPortMap, portNumber: Int)
    : Var[Option[Var[Set[Address]]]] = {
      val unstable = mappings.get(portNumber)
        .map { targetPort =>
          // target may be an int (port number) or string (port name)
          Try(targetPort.toInt).toOption match {
            case Some(targetPortNumber) =>
              // target port is a number and therefore exists
              port(targetPortNumber).map(Some(_))
            case None =>
              // target port is a name and may or may not exist
              port(targetPort)
          }
        }.getOrElse { Var(None) }
      stabilize(unstable)
    }
    def lookupNamedPort(portName: String): Var[Option[Var[Set[Address]]]] =
      stabilize(port(portName))

    private[this] val _endpoints = Var[Set[Endpoint]](endpoints)
    private[this] val _portMap = Var[Map[String, Int]](ports)

    def port(portName: String): Var[Option[Set[Address]]] =
      _portMap.join(_endpoints).map { case ((portState, endpointsState)) =>
        portState.get(portName).map { portNumber =>
          for {
            Endpoint(ip, nodeName) <- endpointsState
            isa = new InetSocketAddress(ip, portNumber)
          } yield Address.Inet(isa, nodeName.map(Metadata.nodeName -> _).toMap)
            .asInstanceOf[Address]
        }
      }

    def port(portNumber: Int): Var[Set[Address]] =
      _endpoints.map { endpointsState =>
        for {
          Endpoint(ip, nodeName) <- endpointsState
          isa = new InetSocketAddress(ip, portNumber)
        } yield Address.Inet(isa, nodeName.map(Metadata.nodeName -> _).toMap)
          .asInstanceOf[Address]
      }
    def update(endpoints: v1.Endpoints): Unit = {
      val (newEndpoints, newPorts) = endpoints.subsets.toEndpointsAndPorts

      synchronized {
        if (newEndpoints != _endpoints.sample()) _endpoints() = newEndpoints
        if (newPorts != _portMap.sample()) _portMap() = newPorts
      }
    }
  }

  protected implicit class RichSubset(val subset: v1.EndpointSubset) extends AnyVal {
    def toPortMap: PortMap =
      (for {
        v1.EndpointPort(port, Some(name), maybeProto) <- subset.portsSeq
        if maybeProto.map(_.toUpperCase).getOrElse("TCP") == "TCP"
      } yield name -> port) (breakOut)

    def toEndpointSet: Set[Endpoint] =
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
        (subset.toEndpointSet, subset.toPortMap)
      }
      val (endpoints, ports) = result.unzip
      (endpoints.flatten.toSet, if (ports.isEmpty) Map.empty else ports.reduce(_ ++ _))
    }
  }

  protected implicit class RichEndpoints(val endpoints: v1.Endpoints) extends AnyVal {
    @inline def cache: EndpointsCache =
      EndpointsCache.tupled(endpoints.subsets.toEndpointsAndPorts)
  }

}