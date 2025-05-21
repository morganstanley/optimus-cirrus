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
import optimus.platform._
import optimus.platform.relational.RelationalException
import optimus.platform.relational.persistence.ConstValueSerializer
import optimus.platform.relational.persistence.RelationTreeDecoder
import optimus.platform.relational.persistence.RelationTreeMessage
import optimus.platform.relational.persistence.protocol.RelExpr.RelationElement.{ElementType => ElemType}
import optimus.platform.relational.tree._
import msjava.slf4jutils.scalalog._

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
 * The Relation tree decoder
 *
 * constructor deserializers is used for backward compatibility.
 *
 * New version will pick the target ProviderPersistenceSerializer from provider relation's protocol buffer field
 * ProviderDeserializerName Old version will use the constructor argument deserializers to find the target
 * ProviderPersistenceSerializer.
 *
 * The main reason is EntityProviderPersistenceSerializer is in platform project but cannot be used here. So when old
 * client protocol buffer containing EntityMultiRelation Provider goes to new server, client protocol doesn't contain
 * ProviderDeserializerName but can use constructor to deserialize entity.
 */
object RelTreeDecoder {
  private val deserializers = List(
    ProviderPersistence.findProviderPersistenceDeserializer(
      "optimus.platform.relational.persistence.protocol.DefaultProviderPersistence"),
    ProviderPersistence.findProviderPersistenceDeserializer(
      "optimus.platform.relational.dal.EntityProviderPersistence"),
    ProviderPersistence.findProviderPersistenceDeserializer(
      "optimus.platform.relational.namespace.NamespaceProviderPersistence")
  )
}

trait RelTreeDecoder extends RelationTreeDecoder {
  import RelTreeDecoder.deserializers

  protected val constValueProtoSerializer: ConstValueSerializer
  protected def createTypeInfoDecoder(source: Seq[RelExpr.TypeInfo]): TypeInfoDecoder = DefaultTypeInfoDecoder(source)

  override def decode(rel: RelationTreeMessage): RelationElement = {
    val r = RelExpr.Relation.parseFrom(rel.toByteArray)
    if (r.getVersion > 1) throw new UnsupportedOperationException("Unsupported query version " + r.getVersion)
    val decodeType = createTypeInfoDecoder(r.getTypeInfosList.asScalaUnsafeImmutable)
    decodeRelation(r.getRoot, decodeType)
  }

  protected def decodeRelation(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): RelationElement =
    if (r == null) null
    else
      r.getNodeType match {
        case ElemType.Method                                => decodeMethod(r, decodeType)
        case ElemType.UnaryExpression                       => decodeUnary(r, decodeType)
        case ElemType.BinaryExpression                      => decodeBinary(r, decodeType)
        case ElemType.Parameter                             => decodeParameter(r, decodeType)
        case ElemType.MemberRef | ElemType.IndexedMemberRef => decodeMemberRef(r, decodeType)
        case ElemType.ConstValue                            => decodeConstant(r, decodeType)
        case ElemType.ForteFuncCall                         => decodeFuncCall(r, decodeType)
        case ElemType.ExpressionList                        => decodeExpressionList(r, decodeType)
        case ElemType.Provider                              => decodeProvider(r, decodeType)
        case ElemType.SerializedValue                       => decodeSerializedValue(r, decodeType)
        case _ => throw new UnsupportedOperationException("Unsupported elementType: " + r.getNodeType)
      }

  protected def decodeSerializedValue(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): ConstValueElement = {
    val serializedValueElement = r.getSerializedValueElement
    val valueByteStr = serializedValueElement.getValue
    val typeInfo = decodeType(serializedValueElement.getTypeInfo)
    val value = constValueProtoSerializer.fromBytes(valueByteStr.toByteArray)
    new ConstValueElement(value, typeInfo)
  }

  protected def decodeMethod(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): MethodElement = {
    val typeInfo = decodeType(r.getItemType)
    val method = r.getMethod
    val queryMethod = QueryMethodNumberMap.getQueryMethodByNumber(method.getQueryMethod.getNumber)
    val args = method.getArgumentsList.asScala map { arg =>
      decodeMethodArg(arg, decodeType)
    }
    val keys = r.getKeysList.asScalaUnsafeImmutable.map(_.getName)
    new MethodElement(queryMethod, args.toList, typeInfo, DynamicKey(keys))
  }

  protected def decodeUnary(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): UnaryExpressionElement = {
    val unary = r.getUnary()
    val operand = decodeRelation(unary.getOperand, decodeType)
    val typeInfo = decodeType(r.getItemType)
    new UnaryExpressionElement(UnaryExpressionType.apply(unary.getExprType.getNumber), operand, typeInfo)
  }

  protected def decodeBinary(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): BinaryExpressionElement = {
    val binary = r.getBinary
    val left = decodeRelation(binary.getLeft, decodeType)
    val right = decodeRelation(binary.getRight, decodeType)
    val typeInfo = decodeType(r.getItemType)
    new BinaryExpressionElement(BinaryExpressionType.apply(binary.getExprType.getNumber), left, right, typeInfo)
  }

  protected def decodeParameter(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): ParameterElement =
    new ParameterElement(r.getParameter.getName, decodeType(r.getItemType))

  protected def decodeMemberRef(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): MemberElement = {
    if (r.hasIndexMemberElement) {
      val im = r.getIndexMemberElement
      val memberName = im.getMemberName
      val unique = im.getUnique
      val instanceProvider = decodeRelation(im.getInstance, decodeType)
      val indexed = im.getIndexed
      val typeName = im.getTypeName
      val refFilter = if (im.hasRefFilter) im.getRefFilter else false
      val propertyNames = im.getPropertyNamesList.asScala
      val typeInfo = decodeType(r.getItemType)

      val metadata = new mutable.HashMap[String, Any]
      metadata.put("unique", unique)
      metadata.put("indexed", indexed)
      metadata.put("typeName", typeName)
      metadata.put("refFilter", refFilter)
      metadata.put("propertyNames", propertyNames)
      val newMemberDescriptor = new RuntimeFieldDescriptor(
        if (instanceProvider != null) instanceProvider.projectedType() else null,
        memberName,
        typeInfo,
        metadata.toMap)
      new MemberElement(instanceProvider, memberName, newMemberDescriptor)
    } else {
      val member = r.getMember
      val memberName = member.getMemberName
      val instanceProvider = decodeRelation(if (member.hasInstance) member.getInstance else null, decodeType)
      val typeInfo = decodeType(r.getItemType)

      val metadata =
        if (member.hasMetadata)
          constValueProtoSerializer.fromBytes(member.getMetadata.toByteArray).asInstanceOf[Map[String, Any]]
        else Map.empty[String, Any]

      val newMemberDescriptor = new RuntimeFieldDescriptor(
        if (instanceProvider != null) instanceProvider.projectedType() else null,
        memberName,
        typeInfo,
        metadata)
      new MemberElement(instanceProvider, memberName, newMemberDescriptor)
    }
  }

  /**
   * it is only used when decoding property value from String to a specific type
   */
  private def decodeAnyType(value: String, typeInfo: TypeInfo[_]): Any = {
    typeInfo match {
      case TypeInfo.LONG    => java.lang.Long.valueOf(value)
      case TypeInfo.INT     => java.lang.Integer.valueOf(value)
      case TypeInfo.BOOLEAN => java.lang.Boolean.valueOf(value)
      case TypeInfo.FLOAT   => java.lang.Float.valueOf(value)
      case TypeInfo.DOUBLE  => java.lang.Double.valueOf(value)
      case TypeInfo.BYTE    => java.lang.Byte.valueOf(value)
      case TypeInfo.CHAR    => value.charAt(0)
      case TypeInfo.SHORT   => java.lang.Short.valueOf(value)
      case TypeInfo.STRING  => value
      case _ =>
        throw new RuntimeException("Only primitive type is allowed in the value field of properties of serializedKey")
    }
  }

  protected def decodeConstant(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): ConstValueElement = {
    val constant = r.getConstValue
    if (constant.hasValue) {
      val typeInfo = decodeType(r.getItemType)
      val name = typeInfo.name
      val value = constValueProtoSerializer.fromBytes(constant.getValue.toByteArray)

      new ConstValueElement(value, typeInfo)
    } else if (constant.hasTypeInfo) new ConstValueElement(decodeType(constant.getTypeInfo))
    else new ConstValueElement(null)
  }

  protected def decodeFuncCall(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): FuncElement = {
    val typeInfo = decodeType(r.getItemType)
    val func = r.getFunc
    val callee = decodeCallee(func.getCallee, decodeType)
    val args = func.getArgumentsList.asScala map { a =>
      decodeMethodArg(a, decodeType).arg
    }
    new FuncElement(callee, args.toList, null)
  }

  protected def decodeExpressionList(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): ExpressionListElement =
    new ExpressionListElement(
      r.getExpressionList.getRelElemsList.asScala.toList map { e =>
        decodeRelation(e, decodeType)
      },
      decodeType(r.getItemType))

  protected def decodeProvider(r: RelExpr.RelationElement, decodeType: TypeInfoDecoder): ProviderRelation = {
    val deserializer = deserializers.find(_.canDeserializable(r))
    if (deserializer.isDefined) deserializer.get.createRelationElement(r, decodeType)
    else {
      if (r.getProvider.hasProviderDeserializerName()) {
        val deserializer =
          ProviderPersistence.findProviderPersistenceDeserializer(r.getProvider.getProviderDeserializerName)
        deserializer.createRelationElement(r, decodeType)
      } else
        throw new RelationalException(
          s"Relation element ${r.getClass} cannot be deserialized from GPB, since it doesn't specify ProviderDeserializerName")
    }
  }

  protected def decodeMethodArg(a: RelExpr.MethodArg, decodeType: TypeInfoDecoder): MethodArg[RelationElement] = {
    val r = if (a.hasRelational) a.getRelational else null
    new MethodArg(a.getName, decodeRelation(r, decodeType))
  }

  protected def decodeCallee(c: RelExpr.Callee, decodeType: TypeInfoDecoder): Callee =
    new Callee(
      decodeType(c.getResType),
      c.getName,
      c.getSigniture,
      c.getArgumentsList.asScala.toList map { i =>
        new Argument(decodeType(i))
      })
}

private[optimus] trait TypeInfoDecoder {
  def source: Seq[RelExpr.TypeInfo]
  def apply(ref: RelExpr.TypeRef): TypeInfo[_]
  protected def decode(index: Int): TypeInfo[_]
  protected def getPrimitiveOrClass(name: String): Option[Class[_]]
}

private[optimus] class DefaultTypeInfoDecoder(override val source: Seq[RelExpr.TypeInfo]) extends TypeInfoDecoder {
  import DefaultTypeInfoDecoder._

  def apply(ref: RelExpr.TypeRef): TypeInfo[_] = {
    val index = ref.getIndex
    typeInfos.get(index).getOrElse {
      val typeInfo = decode(index)
      typeInfos(index) = typeInfo
      typeInfo
    }
  }

  protected def decode(index: Int): TypeInfo[_] = {
    val (t, interfaces, cls, signatures, ctorParams, typeParams) = getTypeInfoProps(index)
    val classes = cls ++: interfaces
    val typeInfo =
      if (classes.isEmpty)
        // no classes were resolved - mock type info
        TypeInfo.mock(
          t.getName,
          signatures,
          ctorParams,
          typeParams
        ) // in case TypeInfo(Nil, Nil, Nil, Nil) name comes from TypeInfo.runtimeClassName which is specified by user through TypeInfo.mock(className)
      else
        // some classes were resolved - create normal type info
        TypeInfo(
          classes,
          signatures,
          ctorParams,
          typeParams
        ) // this includes anonymous type case which cls + interfaces is also Nil, but others are not Nil
    log.debug(s"Decoded type number ${index} to type ${typeInfo.name}")
    typeInfo
  }

  protected def getTypeInfoProps(index: Int) = {
    val t = source(index)
    val interfaces = t.getInterfaceNamesList.asScalaUnsafeImmutable.flatMap(getPrimitiveOrClass)

    val cls = t match {
      case _ if t.getName == TypeInfoEncoder.proxyName =>
        // generate a proxy class for these interfaces
        Some(Proxy.getProxyClass(getClass.getClassLoader, interfaces: _*)): @nowarn("cat=deprecation")
      case _ if t.hasConcreteClassName =>
        getPrimitiveOrClass(t.getConcreteClassName)
      case _ => None
    }

    val signatures = t.getStructuralMethodsList.asScalaUnsafeImmutable.map { s =>
      val split = s.split("\\(")
      Signature(split(0), "(" + split(1))
    }

    val ctorParams = for {
      param <- t.getConstructorParamsList.asScalaUnsafeImmutable
      cls <- getPrimitiveOrClass(param.getClassName)
    } yield (param.getParamName, cls)

    val typeParams = t.getTypeParamsList.asScalaUnsafeImmutable.map(apply)
    (t, interfaces, cls, signatures, ctorParams, typeParams)
  }

  protected def getPrimitiveOrClass(name: String): Option[Class[_]] = {
    name match {
      case "int"     => Some(classOf[Int])
      case "boolean" => Some(classOf[Boolean])
      case "short"   => Some(classOf[Short])
      case "long"    => Some(classOf[Long])
      case "float"   => Some(classOf[Float])
      case "double"  => Some(classOf[Double])
      case "byte"    => Some(classOf[Byte])
      case "char"    => Some(classOf[Char])
      case _ =>
        try {
          Some(Class.forName(name))
        } catch {
          case e: Exception => log.info("Class.forName cannot find class {}", name); None
        }
    }
  }

  // memoize TypeInfo
  private val typeInfos = mutable.Map[Int, TypeInfo[_]](-1 -> null.asInstanceOf[TypeInfo[_]])
}

private[optimus] object DefaultTypeInfoDecoder {
  def apply(source: Seq[RelExpr.TypeInfo]) = new DefaultTypeInfoDecoder(source)
  val log = getLogger[DefaultTypeInfoDecoder]
}
