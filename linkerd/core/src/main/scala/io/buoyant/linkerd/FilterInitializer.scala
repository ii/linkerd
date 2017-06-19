package io.buoyant.linkerd

import io.buoyant.config.{PolymorphicConfig, ConfigInitializer}

abstract class FilterInitializer extends ConfigInitializer

abstract class FilterConfig extends PolymorphicConfig {}

object FilterConfig {}
