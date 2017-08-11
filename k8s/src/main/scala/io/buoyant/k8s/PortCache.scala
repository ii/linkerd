package io.buoyant.k8s

import java.net.InetSocketAddress
import com.twitter.finagle.Address
import com.twitter.util.{Activity, Var}
import io.buoyant.k8s.Ns.ObjectCache
import scala.collection.mutable

class PortCache extends ObjectCache[v1.Service, v1.ServiceWatch] {
  //  private[this] var ports = Var(mutable.Map.empty[String, Address])
  private[this] var portMap = mutable.Map.empty[Int, String]

  private[this] def addPorts(service: v1.Service): Unit =
  // for each port in the
    for {
      meta <- service.metadata.toSeq
      name <- meta.name.toSeq
      status <- service.status.toSeq
      lb <- status.loadBalancer.toSeq
      spec <- service.spec.toSeq
      v1.ServicePort(port, targetPort, portName) <- spec.ports
    } {

      //      // for each hostname in the service's ingresses, map `portName` to `hostname:port`
      //      for {
      //        ingress <- lb.ingressSeq
      //        hostname <- ingress.hostname.orElse(ingress.ip)
      //      } ports() += portName -> Address(new InetSocketAddress(hostname, port))

      // then, if the port is remapped to a target port, map the port
      // to the target port; otherwise, map the port to itself.
      portMap += port -> targetPort.getOrElse(port.toString)
    }

  /**
   * Look up the port mapping for a given port, returning the
   * port that port is mapped to.
   *
   * @param port
   * @return
   */
  def get(port: Int): Option[String] = synchronized {
    portMap.get(port)
  }

  //  /**
  //   * Look up the address for a given port name
  //   * port that port is mapped to.
  //   * @param port
  //   * @return
  //   */
  //  def getAddress(port: String): Var[Option[Var[Address]]] = synchronized {
  //    val unstable = ports.map { ports => ports.get(port) }
  //    stabilize(unstable)
  //  }

  override def initialize(init: v1.Service): Unit =
    synchronized {
      addPorts(init)
    }

  override def update(watch: v1.ServiceWatch): Unit = synchronized {
    watch match {
      case v1.ServiceAdded(service) => addPorts(service)
      case v1.ServiceModified(service) => addPorts(service)
      case _ => ???
    }
  }

}

object PortCache {
  private[this] val initPortCache: v1.Service => PortCache = s => {
    val pc = new PortCache
    pc.initialize(s)
    pc
  }

  def fromService(service: NsObjectResource[v1.Service, v1.ServiceWatch]): Activity[PortCache]
  = service.activity(initPortCache) { (cache, event) => cache.update(event); cache }
}