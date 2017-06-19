package io.buoyant.linkerd.protocol

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.http.{Request, Response}
import io.buoyant.config.PolymorphicConfig

abstract class HttpFilterConfig extends PolymorphicConfig {
  @JsonIgnore
  def newFilter(): SimpleFilter[Request, Response]
}
