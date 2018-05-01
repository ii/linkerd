package io.buoyant.linkerd

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter.EndpointAddr
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param.Label
import com.twitter.io.Buf
import com.twitter.util.Future

class RequestTestResolver(dst: EndpointAddr, lbl: Label) extends SimpleFilter[Request, Response] {
  override def apply(
    req: Request,
    svc: Service[Request, Response]
  ): Future[Response] = {

    getResolverHeader(req) match {
      case None => svc(req)
      case Some(header) =>
        val resp = Response()
        resp.content(Buf.Utf8(s"request identified as ${lbl.label}: Endpoint=${dst.addr.toString}"))
        Future.value(resp)
    }
  }

  def getResolverHeader(req: Request) = {
    req.headerMap.get("request-resolver")
  }
}

object RequestTestResolver {
  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module3[EndpointAddr, Label, LoadBalancerFactory.Dest, ServiceFactory[Request, Response]] {

      override def role: Stack.Role = Stack.Role("RequestResolver")

      override def description: String = "Intercepts to respond with useful client destination info"

      override def make(dst: EndpointAddr, label: Label, lbDest: LoadBalancerFactory.Dest, next: ServiceFactory[Request, Response]): ServiceFactory[Request, Response] =

        new RequestTestResolver(dst, label).andThen(next)
    }
}

