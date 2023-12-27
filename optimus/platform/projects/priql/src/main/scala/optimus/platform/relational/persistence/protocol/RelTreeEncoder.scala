/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform.relational.persistence.protocol

import java.lang.reflect.Proxy
import com.google.protobuf.ByteString
import optimus.platform.relational.RelationalException
import optimus.platform.relational.inmemory.ScalaTypeMultiRelation
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.persistence.{ConstValueSerializer, RelationTreeEncoder, RelationTreeMessage}
import optimus.platform.relational.persistence.protocol.RelExpr.BinaryExpressionElement.BinaryExpressionType
import optimus.platform.relational.persistence.protocol.RelExpr.MethodElement.QueryMethodType
import optimus.platform.relational.persistence.protocol.RelExpr.RelationElement.{ElementType, Builder => RelBuilder}
import optimus.platform.relational.persistence.protocol.RelExpr.TypeRef
import optimus.platform.relational.persistence.protocol.RelExpr.UnaryExpressionElement.UnaryExpressionType
import optimus.platform.relational.tree._
import optimus.platform.util.ProxyMarker

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
 * The relation tree encoder
 */
trait RelTreeEncoder extends RelationTreeEncoder {

  protected val constValueProtoSerializer: ConstValueSerializer

  override def encode(relation: RelationElement): RelationTreeMessage = {
    val encodeType = new TypeInfoEncoder()

    val rel = RelExpr.Relation.newBuilder
      .setVersion(1)
      .setRoot(encodeRelation(relation, encodeType))
      .addAllTypeInfos(encodeType.types.asJava)
      .build()

    RelationTreeMessage(rel.toByteArray)
  }

  protected def encodeRelation(relation: RelationElement, encodeType: TypeInfoEncoder): RelBuilder =
    relation match {
      case ele: MethodElement           => encodeMethod(ele, encodeType)
      case ele: UnaryExpressionElement  => encodeUnary(ele, encodeType)
      case ele: BinaryExpressionElement => encodeBinary(ele, encodeType)
      case ele: ParameterElement        => encodeParameter(ele, encodeType)
      case ele: MemberElement           => encodeMemberRef(ele, encodeType)
      case ele: ConstValueElement       => encodeConstant(ele, encodeType)
      case ele: FuncElement             => encodeFuncCall(ele, encodeType)
      case ele: ExpressionListElement   => encodeExpressionList(ele, encodeType)
      case ele: ProviderRelation        => encodeProvider(ele, encodeType)
      case _ => throw new UnsupportedOperationException("Unsupported elementType: " + relation.elementType)
    }

  protected def encodeMethod(m: MethodElement, encodeType: TypeInfoEncoder): RelBuilder = {
    val method = RelExpr.MethodElement.newBuilder.setQueryMethod(encodeQueryMethod(m.methodCode))
    for {
      args <- Option(m.methodArgs)
      arg <- args
    } method.addArguments(encodeMethodArg(arg, encodeType))

    val builder = RelExpr.RelationElement.newBuilder
    for (mkf <- Option(m.key.fields); k <- mkf)
      builder.addKeys(RelExpr.RelationKey.newBuilder.setName(k).build())

    builder
      .setNodeType(ElementType.Method)
      .setItemType(encodeType(m.projectedType()))
      .setMethod(method)
  }

  protected def encodeUnary(u: UnaryExpressionElement, encodeType: TypeInfoEncoder): RelBuilder = {
    RelExpr.RelationElement.newBuilder
      .setNodeType(ElementType.UnaryExpression)
      .setItemType(encodeType(u.projectedType()))
      .setUnary(
        RelExpr.UnaryExpressionElement.newBuilder
          .setExprType(UnaryExpressionType.forNumber(u.op.id))
          .setOperand(encodeRelation(u.element, encodeType)))
  }

  protected def encodeBinary(b: BinaryExpressionElement, encodeType: TypeInfoEncoder): RelBuilder =
    RelExpr.RelationElement.newBuilder
      .setNodeType(ElementType.BinaryExpression)
      .setItemType(encodeType(b.projectedType()))
      .setBinary(
        RelExpr.BinaryExpressionElement.newBuilder
          .setExprType(BinaryExpressionType.forNumber(b.op.id))
          .setLeft(encodeRelation(b.left, encodeType))
          .setRight(encodeRelation(b.right, encodeType)))

  protected def encodeParameter(p: ParameterElement, encodeType: TypeInfoEncoder): RelBuilder =
    RelExpr.RelationElement.newBuilder
      .setNodeType(ElementType.Parameter)
      .setItemType(encodeType(p.projectedType()))
      .setParameter(RelExpr.ParameterElement.newBuilder.setName(p.parameterName))

  protected def encodeMemberRef(m: MemberElement, encodeType: TypeInfoEncoder): RelBuilder = {
    val elType = ElementType.MemberRef

    // old client code
    if (m.member != null && !m.member.metadata.isEmpty && m.member.metadata.get("unique") != None) {
      val indexedMember = RelExpr.IndexedMemberElement.newBuilder.setMemberName(m.memberName)
      Option(m.instanceProvider) foreach { mip =>
        indexedMember.setInstance(encodeRelation(mip, encodeType))
      }
      indexedMember.setUnique(m.member.metadata.get("unique").get.asInstanceOf[Boolean])
      indexedMember.setIndexed(m.member.metadata.get("indexed").get.asInstanceOf[Boolean])
      indexedMember.setTypeName(m.member.metadata.get("typeName").get.asInstanceOf[String])
      indexedMember.setRefFilter(m.member.metadata.get("refFilter").get.asInstanceOf[Boolean])
      indexedMember.addAllPropertyNames(m.member.metadata.get("propertyNames").get.asInstanceOf[Seq[String]].asJava)

      RelExpr.RelationElement.newBuilder
        .setNodeType(elType)
        .setItemType(encodeType(m.projectedType()))
        .setIndexMemberElement(indexedMember)
    } else {
      val member = RelExpr.MemberElement.newBuilder.setMemberName(m.memberName)

      Option(m.instanceProvider) foreach { mip =>
        member.setInstance(encodeRelation(mip, encodeType))
      }

      if (m.member != null && !m.member.metadata.isEmpty)
        member.setMetadata(ByteString.copyFrom(constValueProtoSerializer.toBytes(m.member.metadata)))

      RelExpr.RelationElement.newBuilder
        .setNodeType(elType)
        .setItemType(encodeType(m.projectedType()))
        .setMember(member)
    }

  }

  /**
   * it is only used when encoding property value in serializedKeys, we need to encode type of the property value and
   * return its TypeRef
   */
  private def encodeAnyType(value: Any, encodeType: TypeInfoEncoder): TypeRef = {
    value match {
      case _: java.lang.Long | _: Long       => encodeType(TypeInfo.LONG)
      case _: java.lang.Integer | _: Int     => encodeType(TypeInfo.INT)
      case _: java.lang.Boolean | _: Boolean => encodeType(TypeInfo.BOOLEAN)
      case _: java.lang.Float | _: Float     => encodeType(TypeInfo.FLOAT)
      case _: java.lang.Double | _: Double   => encodeType(TypeInfo.DOUBLE)
      case _: java.lang.Byte | _: Byte       => encodeType(TypeInfo.BYTE)
      case _: java.lang.Character | _: Char  => encodeType(TypeInfo.CHAR)
      case _: java.lang.Short | _: Short     => encodeType(TypeInfo.SHORT)
      case _: String                         => encodeType(TypeInfo.STRING)
      case _ =>
        throw new RuntimeException("only primitive type is allowed in the value field of properties of serializedKey")
    }
  }

  protected def encodeConstant(c: ConstValueElement, encodeType: TypeInfoEncoder): RelBuilder = {
    if (c.serializedValue) {
      val serializedValueElement = RelExpr.SerializedValueElement.newBuilder
        .setValue(ByteString.copyFrom(constValueProtoSerializer.toBytes(c.value)))
        .setTypeInfo(encodeType(c.rowTypeInfo))

      RelExpr.RelationElement.newBuilder
        .setNodeType(ElementType.SerializedValue)
        .setItemType(encodeType(c.projectedType()))
        .setSerializedValueElement(serializedValueElement)
    } else {
      val constant = RelExpr.ConstValueElement.newBuilder
      c.value match {
        case null           => ()
        case v: TypeInfo[_] => constant.setTypeInfo(encodeType(v))
        case v: Class[_]    => constant.setTypeInfo(encodeType(c.rowTypeInfo))
        case v =>
          try (constant.setValue(ByteString.copyFrom(constValueProtoSerializer.toBytes(v))))
          catch {
            case e: IllegalArgumentException =>
              // ProtoPickleSerializer doesn't support class instances serialization, we need to serialize them
              // using RelationalUtils.convertConstToString(v) and then fill it into FieldProto.
              // and convert it to ByteString. in fact we cannot support class instance serialization in terms of
              // Relation tree encode and decode. Even if we use RelationalUtils.convertConstToString(v) and
              // convertStringToConst.
              constant.setValue(
                ByteString.copyFrom(constValueProtoSerializer.toBytes(RelationalUtils.convertConstToString(v))))
          }
      }

      RelExpr.RelationElement.newBuilder
        .setNodeType(ElementType.ConstValue)
        .setItemType(encodeType(c.projectedType()))
        .setConstValue(constant)
    }
  }

  protected def encodeFuncCall(f: FuncElement, encodeType: TypeInfoEncoder): RelBuilder = {
    val func = RelExpr.FuncElement.newBuilder.setCallee(encodeCallee(f.callee, encodeType))
    for {
      args <- Option(f.arguments)
      arg <- args
    } encodeMethodArg(new MethodArg[RelationElement]("", arg), encodeType)

    RelExpr.RelationElement.newBuilder
      .setNodeType(ElementType.ForteFuncCall)
      .setItemType(encodeType(f.projectedType()))
      .setFunc(func)
  }

  protected def encodeExpressionList(ele: ExpressionListElement, encodeType: TypeInfoEncoder): RelBuilder = {
    val relations = RelExpr.ExpressionListElement.newBuilder
    for (rel <- ele.exprList)
      relations.addRelElems(encodeRelation(rel, encodeType))

    RelExpr.RelationElement.newBuilder
      .setNodeType(ElementType.ExpressionList)
      .setItemType(encodeType(ele.projectedType()))
      .setExpressionList(relations)
  }

  protected def encodeProvider(p: ProviderRelation, encodeType: TypeInfoEncoder): RelBuilder =
    p match {
      case _: ScalaTypeMultiRelation[_] =>
        DefaultProviderPersistence.createRelationElementBuilder(p, encodeType)
      case c: ProviderPersistenceSupport => c.serializerForThisProvider.createRelationElementBuilder(c, encodeType)
      case _ =>
        throw new RelationalException(
          s"this provider ${p.getClass} cannot be serialized into GPB since there is no ProviderPersistenceSerializer provided")
    }

  protected def encodeMethodArg(arg: MethodArg[RelationElement], encodeType: TypeInfoEncoder): RelExpr.MethodArg = {
    val b = RelExpr.MethodArg.newBuilder.setName(arg.name)
    Option(arg.param).foreach(ap => b.setRelational(encodeRelation(ap, encodeType)))
    b.build()
  }

  protected def encodeCallee(callee: Callee, encodeType: TypeInfoEncoder): RelExpr.Callee = {
    val builder = RelExpr.Callee.newBuilder
      .setName(Option(callee.name).getOrElse(""))
      .setSigniture(Option(callee.signature).getOrElse(""))
      .setResType(encodeType(callee.resType))

    for {
      args <- Option(callee.arguments)
      arg <- args
    } builder.addArguments(encodeType(arg.argType))

    builder.build()
  }

  protected def encodeQueryMethod(qm: QueryMethod): QueryMethodType =
    Option(QueryMethodType.forNumber(QueryMethodNumberMap.getNumberByQueryMethod(qm))) getOrElse {
      throw new UnsupportedOperationException("Unsupported QueryMethod: " + qm)
    }
}

private[optimus] final class TypeInfoEncoder {
  def apply(typeInfo: TypeInfo[_]): RelExpr.TypeRef = {
    typesIndex.get(typeInfo).getOrElse {
      val tb = RelExpr.TypeInfo.newBuilder
        .setName(typeInfo.name) /*.setCategory(encodeCategory(typeInfo.category))*/
        .setId(0)

      for (itfc <- typeInfo.interfaces) tb.addInterfaceNames(itfc.getName)
      if (typeInfo.clazz != null && (Proxy.isProxyClass(typeInfo.clazz) || ProxyMarker.isProxyClass(typeInfo.clazz))) {
        tb.setName(TypeInfoEncoder.proxyName)
        for (itfc <- ProxyMarker.getInterfaces(typeInfo.clazz))
          tb.addInterfaceNames(itfc.getName)
      }

      for (c <- typeInfo.concreteClass) tb.setConcreteClassName(c.getName)
      for (m <- typeInfo.pureStructuralMethods) tb.addStructuralMethods(m.toString)
      for (tp <- typeInfo.typeParams) tb.addTypeParams(apply(tp))
      for (c <- typeInfo.primaryConstructorParams)
        tb.addConstructorParams(
          RelExpr.ConstructorParamsInfo.newBuilder.setParamName(c._1).setClassName(c._2.getName).build())

      // we do this only for backward compatibility with legacy server versions: category is no longer used
      tb.setCategory(RelExpr.TypeInfo.Category.SCALA_OBJ)
      val ref = RelExpr.TypeRef.newBuilder().setIndex(typesBuffer.length).build()
      typesBuffer += tb.build
      typesIndex(typeInfo) = ref
      ref
    }
  }

  def types = typesBuffer.toSeq

  private val typesBuffer = mutable.Buffer[RelExpr.TypeInfo]()
  private val typesIndex =
    mutable.Map[TypeInfo[_], RelExpr.TypeRef](null.asInstanceOf[TypeInfo[_]] -> TypeInfoEncoder.nullTypeRef)
}

object TypeInfoEncoder {
  val proxyName = "[Proxy]"
  private val nullTypeRef = RelExpr.TypeRef.newBuilder().setIndex(-1).build()
}
