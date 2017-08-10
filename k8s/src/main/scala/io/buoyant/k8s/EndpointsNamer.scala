package io.buoyant.k8s

import java.net.InetAddress
import com.twitter.conversions.time._
import com.twitter.finagle.{Service => _, _}
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import scala.collection.mutable

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
        val portCache = PortCache.fromService(mkApi(nsName).service(serviceName))
        lookupServices(nsName, portName, serviceName, portCache, id, residual)

      case (id@Path.Utf8(nsName, portName, serviceName, labelValue), Some(label)) =>
        val residual = path.drop(variablePrefixLength)
        log.debug("k8s lookup: %s %s %s", id.show, label, path.show)
        val labelSelector = Some(s"$label=$labelValue")
        val portCache = PortCache.fromService(mkApi(nsName).service(serviceName))
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
        val portCache = PortCache.fromService(mkApi(nsName).service(serviceName))
        lookupServices(nsName, portName, serviceName, portCache, id, residual)

      case (id@Path.Utf8(portName, serviceName, labelValue), Some(label)) =>
        val residual = path.drop(variablePrefixLength)
        log.debug("k8s lookup: %s %s %s", id.show, label, path.show)
        val labelSelector = Some(s"$label=$labelValue")
        val portCache = PortCache.fromService(mkApi(nsName).service(serviceName))
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

  private[k8s] def lookupServices(
    nsName: String,
    portName: String,
    serviceName: String,
    portCacheAct: Activity[PortCache],
    id: Path,
    residual: Path,
    labelSelector: Option[String] = None
  ): Activity[NameTree[Name]] = {

    val ns = mkApi(nsName)

    def endpointsAct(targetPortName: String): Activity[NameTree[Name]] = {

      def mkNameTree: NameTree[Name] =
        cache.get(nsName, targetPortName, serviceName) match {
          case Some(addr) => NameTree.Leaf(Name.Bound(addr, idPrefix ++ id, residual))
          case None => NameTree.Neg
        }

      @inline def initCache(endpoints: v1.Endpoints): NameTree[Name] = {
        // wish i didn't have to call initialize then get here, just to get the addrs
        // from the Endpoints object. who wrote this api anyway? /me raises hand. whoops.
        cache.initialize(endpoints)
        mkNameTree
      }


      ns.endpoints(serviceName)
        .activity(initCache, labelSelector = labelSelector) { (_, event) =>
          // and similarly update then get is not great programming either
          cache.update(event)
          mkNameTree
        }
    }

    Try(portName.toInt).toOption match {
      case Some(portNumber) =>
        portCacheAct.flatMap { portCache =>
          val portStates: Var[Activity.State[NameTree[Name]]] =
            cache.lookupNumberedPort(nsName, serviceName, portNumber, portCache)
              .map {
                case Some(addr) =>
                  val nt = NameTree.Leaf(Name.Bound(addr, idPrefix ++ id, residual))
                  Activity.Ok(nt)
                case None => Activity.Ok(NameTree.Neg)
              }
          Activity(portStates)
        }
      case None => endpointsAct(portName)
    }


  }
}

object EndpointsNamer {
  val DefaultBackoff: Stream[Duration] =
    Backoff.exponentialJittered(10.milliseconds, 10.seconds)
}

class EndpointsCache extends Ns.ObjectCache[v1.Endpoints, v1.EndpointsWatch] {
  import EndpointsCache._

  private[this] var cache = Map.empty[CacheKey, VarUp[Addr]]
  private[this] var portNameCache = Map.empty[Int, String]

  def getNumberedPort(nsName: String, serviceName: String, portNumber: Int): Option[VarUp[Addr]]
  = portNameCache.get(portNumber).flatMap { portName =>
    cache.get(CacheKey(nsName, portName, serviceName))
  }

  /**
   * For a given port number, apply the port mapping of the service.  The target port of the port
   * mapping may be a named port and the named port may or may not exist.  The outer Var[Option]
   * of the return type tracks whether the port exists and the inner Var[Addr] tracks the actual
   * endpoints if the port does exist.
   */
  private[k8s] def lookupNumberedPort(
    nsName: String,
    serviceName: String,
    portNumber: Int,
    portCache: PortCache
  ): Var[Option[Var[Addr]]] =
    portCache.get(portNumber).flatMap {
      case Some(targetPort) =>
        targetPort.flatMap { target => Var(get(nsName, target, serviceName)) }
      case None => Var(None)
    }

  def initialize(endpoints: v1.Endpoints): Unit =
    add(endpoints)

  override def update(watch: v1.EndpointsWatch): Unit =
    watch match {
      case v1.EndpointsError(e) => log.error("k8s watch error: %s", e)
      case v1.EndpointsAdded(endpoints) => add(endpoints)
      case v1.EndpointsModified(endpoints) => modify(endpoints)
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
    for {(num, name) <- portsMap} synchronized {
      // TODO: this could just be `portNameCache ++= portsMap`, but I
      //       thought the logging was nice?
      log.debug(s"k8s ns %s added port '$name' -> $num")
      portNameCache += num -> name
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
    for {(num, name) <- portsMap} synchronized {
      log.info(s"k8s modified port '$name' -> $num")
      // TODO: this could just be `portNameCache ++= portsMap`, but I
      //       thought the logging was nice?
      portNameCache += (num -> name)
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
    for {(key, _) <- portsMap} synchronized {
      portNameCache -= key
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
