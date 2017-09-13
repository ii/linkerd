package io.buoyant.k8s.istio

import com.twitter.finagle.{Filter, Service}
import com.twitter.util.{Duration, Stopwatch, Try}
import io.buoyant.k8s.istio.mixer.MixerClient

trait IstioRequestAuthorizerFilter[Req, Resp] extends Filter[Req, Resp, Req, Resp] {

  def mixerClient: MixerClient

  def report(request: IstioRequest[Req], response: IstioResponse[Resp], duration: Duration) = {

    mixerClient.report(
      response.responseCode,
      request.requestedPath,
      response.targetService,
      request.sourceLabel,
      request.targetLabel,
      response.responseDuration
    )
  }

  def toIstioRequest(req: Req): IstioRequest[Req]

  def toIstioResponse(resp: Try[Resp], duration: Duration): IstioResponse[Resp]

  def apply(req: Req, svc: Service[Req, Resp]) = {
    val istioRequest = toIstioRequest(req)

    val elapsed = Stopwatch.start()

    svc(req).respond { resp =>

      val duration = elapsed()
      val istioResponse = toIstioResponse(resp, duration)

      val _ = report(istioRequest, istioResponse, duration)
    }
  }
}
