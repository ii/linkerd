package io.buoyant.linkerd.protocol.h2.istio

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle._
import com.twitter.finagle.buoyant.h2
import com.twitter.finagle.buoyant.h2.{Request, Response}
import com.twitter.util.{Duration, Try}
import io.buoyant.config.types.Port
import io.buoyant.k8s.istio.mixer.MixerClient
import io.buoyant.k8s.istio.{IstioConfigurator, IstioRequestAuthorizerFilter, _}
import io.buoyant.linkerd.RequestAuthorizerInitializer
import io.buoyant.linkerd.protocol.h2.H2RequestAuthorizerConfig

class IstioRequestAuthorizer(val mixerClient: MixerClient, params: Stack.Params) extends IstioRequestAuthorizerFilter[Request, Response] {
  override def toIstioRequest(req: Request) = H2IstioRequest(req)

  override def toIstioResponse(resp: Try[Response], duration: Duration) = H2IstioResponse(resp, duration)

  override def toFailedResponse(code: Int, reason: String) = Response(h2.Status(code), h2.Stream.const(reason))
}

case class IstioRequestAuthorizerConfig(
  mixerHost: Option[String] = Some(DefaultMixerHost),
  mixerPort: Option[Port] = Some(Port(DefaultMixerPort))
) extends H2RequestAuthorizerConfig with IstioConfigurator {

  @JsonIgnore
  override def role = Stack.Role("IstioRequestAuthorizer")

  @JsonIgnore
  override def description = "Checks if request is authorised"

  @JsonIgnore
  override def parameters = Seq()

  @JsonIgnore
  def mk(params: Stack.Params): Filter[Request, Response, Request, Response] = {
    new IstioRequestAuthorizer(mkMixerClient(mixerHost, mixerPort), params)
  }
}

class IstioRequestAuthorizerInitializer extends RequestAuthorizerInitializer {
  val configClass = classOf[IstioRequestAuthorizerConfig]
  override val configId = "io.l5d.k8s.istio"
}

object IstioRequestAuthorizerInitializer extends IstioRequestAuthorizerInitializer
