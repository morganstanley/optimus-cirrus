package optimus.platform.dsi.bitemporal.proto

object Prc {
  trait MessageLiteImpl extends com.google.protobuf.MessageLite {
    def getParserForType(): com.google.protobuf.Parser[_ <: com.google.protobuf.MessageLite] = ???
    def getSerializedSize(): Int = ???
    def newBuilderForType(): com.google.protobuf.MessageLite.Builder = ???
    def toBuilder(): com.google.protobuf.MessageLite.Builder = ???
    def toByteArray(): Array[Byte] = ???
    def toByteString(): com.google.protobuf.ByteString = ???
    def writeDelimitedTo(output: java.io.OutputStream): Unit = ???
    def writeTo(output: java.io.OutputStream): Unit = ???
    def writeTo(output: com.google.protobuf.CodedOutputStream): Unit = ???
    def getDefaultInstanceForType(): com.google.protobuf.MessageLite = ???
    def isInitialized(): Boolean = ???
  }

  class PrcUserOptionsProto extends MessageLiteImpl
  class NormalizedCacheableQueryProto extends MessageLiteImpl
  class NonTemporalPrcKeyProto extends MessageLiteImpl
  class PrcClientSessionInfoProto extends MessageLiteImpl
  class NormalizedNonCacheableCommandProto extends MessageLiteImpl
  class SilverKingTraceIdProto extends MessageLiteImpl
  class PrcSingleKeyResponseProto extends MessageLiteImpl
}
