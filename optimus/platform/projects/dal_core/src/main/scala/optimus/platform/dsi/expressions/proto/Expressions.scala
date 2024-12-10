package optimus.platform.dsi.expressions.proto

object Expressions {
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

  class ExpressionProto extends MessageLiteImpl
  class EntityBitempSpaceProto extends MessageLiteImpl
  object EntityBitempSpaceProto {
    class Kind
  }
  class EntityProto extends MessageLiteImpl
  class EventProto extends MessageLiteImpl
  class LinkageProto extends MessageLiteImpl
  class EmbeddableProto extends MessageLiteImpl
  class PropertyProto extends MessageLiteImpl
  class ConstantProto extends MessageLiteImpl
  class BinaryProto extends MessageLiteImpl
  class InProto extends MessageLiteImpl
  class SelectProto extends MessageLiteImpl
  class JoinProto extends MessageLiteImpl
  class MemberProto extends MessageLiteImpl
  class FunctionProto extends MessageLiteImpl
  class UnaryProto extends MessageLiteImpl
  class ConditionProto extends MessageLiteImpl
  class AggregateProto extends MessageLiteImpl
  class ScalarProto extends MessageLiteImpl
  class ExistsProto extends MessageLiteImpl
  class TypeCodeProto extends MessageLiteImpl
  class TemporalityProto extends MessageLiteImpl
  class ValidTimeIntervalProto extends MessageLiteImpl
  class TimeIntervalProto extends MessageLiteImpl
  class InstantProto extends MessageLiteImpl
  class PropertyDefProto extends MessageLiteImpl
  class SortByDefProto extends MessageLiteImpl
}