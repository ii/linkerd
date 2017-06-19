package io.buoyant.linkerd.protocol.http

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.Future
import io.buoyant.config.types.Port
import io.buoyant.linkerd.FilterInitializer
import io.buoyant.linkerd.protocol.HttpFilterConfig

class IstioFilter() extends SimpleFilter[Request, Response] {

  def apply(req: Request, svc: Service[Request, Response]) = {
    svc(req).flatMap { res =>
      Future.value(res)
    }
  }
}

case class IstioFilterConfig(
  mixerHost: Option[String],
  mixerPort: Option[Port]
) extends HttpFilterConfig {
  @JsonIgnore
  val DefaultMixerHost = "istio-mixer.default.svc.cluster.local"
  @JsonIgnore
  val DefaultMixerPort = 9091

  override def newFilter(): SimpleFilter[Request, Response] = {
    val host = mixerHost.getOrElse(DefaultMixerHost)
    val port = mixerPort.map(_.port).getOrElse(DefaultMixerPort)
    new IstioFilter()
  }
}

object IstioFilterConfig {
  val kind = "io.l5d.istio"
}

class IstioFilterInitializer extends FilterInitializer {
  val configClass = classOf[IstioFilterConfig]
  override val configId = IstioFilterConfig.kind
}

object IstioFilterInitializer extends IstioFilterInitializer
