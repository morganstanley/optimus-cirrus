package optimus

object ScalacOptions {
  val common = Seq(
    "-language:postfixOps",
    "-Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq"
  )
  val macros = Seq("-language:experimental.macros")
  val dynamics = Seq("-language:dynamics")
}
