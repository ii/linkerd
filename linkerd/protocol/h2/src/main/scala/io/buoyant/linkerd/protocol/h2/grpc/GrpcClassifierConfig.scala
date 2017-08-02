package io.buoyant.linkerd.protocol.h2.grpc

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import io.buoyant.linkerd.protocol.h2._

@JsonSubTypes(Array(
  new Type(
    value = classOf[NeverRetryableConfig],
    name = "io.l5d.h2.grpc.neverRetryable"
  ),
  new Type(
    value = classOf[AlwaysRetryableConfig],
    name = "io.l5d.h2.grpc.alwaysRetryable"
  ),
  new Type(
    value = classOf[DefaultConfig],
    name = "io.l5d.h2.grpc.default"
  ),
  new Type(
    value = classOf[RetryableStatusCodesConfig],
    name = "io.l5d.h2.grpc.retryableStatusCodes"
  )
))
abstract class GrpcClassifierConfig extends H2ClassifierConfig {
  override def mk: GrpcClassifier
}

class NeverRetryableConfig extends GrpcClassifierConfig {
  override def mk: GrpcClassifier = GrpcClassifiers.NeverRetryable
}

class NeverRetryableInitializer extends H2ClassifierInitializer {
  val configClass = classOf[NeverRetryableConfig]
  override val configId = "io.l5d.h2.grpc.neverRetryable"
}

object NeverRetryableInitializer extends NeverRetryableInitializer

class AlwaysRetryableConfig extends GrpcClassifierConfig {
  override def mk: GrpcClassifier = GrpcClassifiers.NeverRetryable
}

class AlwaysRetryableInitializer extends H2ClassifierInitializer {
  val configClass = classOf[NeverRetryableConfig]
  override val configId = "io.l5d.h2.grpc.alwaysRetryable"
}

object AlwaysRetryableInitializer extends AlwaysRetryableInitializer

class DefaultConfig extends GrpcClassifierConfig {
  override def mk: GrpcClassifier = GrpcClassifiers.Default
}

class DefaultInitializer extends H2ClassifierInitializer {
  val configClass = classOf[DefaultConfig]
  override val configId = "io.l5d.h2.grpc.Default"
}

object DefaultInitializer extends DefaultInitializer

// TODO: support parsing the status codes by name rather than by number?
class RetryableStatusCodesConfig(val retryableStatusCodes: Set[Int]) extends GrpcClassifierConfig {
  override def mk: GrpcClassifier = new GrpcClassifiers.RetryableStatusCodes(retryableStatusCodes)
}

class RetryableStatusCodesInitializer extends H2ClassifierInitializer {
  val configClass = classOf[RetryableStatusCodesConfig]
  override val configId = "io.l5d.h2.grpc.retryableStatusCodes"
}

object RetryableStatusCodesInitializer extends RetryableStatusCodesInitializer