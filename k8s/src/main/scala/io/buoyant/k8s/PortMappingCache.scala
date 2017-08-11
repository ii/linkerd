package io.buoyant.k8s

import com.twitter.util.Activity
import io.buoyant.k8s.Ns.ObjectCache

/**
 * A simple cache storing port remappings on a Kubernetes `Service`
 */
class PortMappingCache extends ObjectCache[v1.Service, v1.ServiceWatch] {
  private[this] var portMap = Map.empty[Int, String]

  private[this] def addPorts(service: v1.Service): Unit =
    for {
      meta <- service.metadata.toSeq
      status <- service.status.toSeq
      spec <- service.spec.toSeq
      v1.ServicePort(port, targetPort, _) <- spec.ports
    } portMap += port -> targetPort.getOrElse(port.toString)


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

  override def initialize(init: v1.Service): Unit =
    synchronized {
      addPorts(init)
    }

  override def update(watch: v1.ServiceWatch): Unit = synchronized {
    watch match {
      case v1.ServiceAdded(service) => addPorts(service)
      case v1.ServiceModified(service) => addPorts(service)
      case v1.ServiceDeleted(service) => ???
      case v1.ServiceError(error) => log.error(s"k8s service watch error $error")
    }
  }

  @inline def apply(port: Int): Option[String] = get(port)

}

object PortMappingCache {
  private[this] val initPortCache: v1.Service => PortMappingCache = s => {
    val pc = new PortMappingCache
    pc.initialize(s)
    pc
  }

  def fromService(service: NsObjectResource[v1.Service, v1.ServiceWatch]): Activity[PortMappingCache] =
    service.activity(initPortCache) { (cache, event) => cache.update(event); cache }
}