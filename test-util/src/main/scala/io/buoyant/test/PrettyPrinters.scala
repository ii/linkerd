package io.buoyant.test

object PrettyPrinters {
  implicit class PrettySeq(val s: TraversableOnce[_]) extends AnyVal {
    def prettyPrint: String = s.mkString(s"\n${s.getClass.getSimpleName}: [\n\t", ",\n\t", "\n]")
  }
}
