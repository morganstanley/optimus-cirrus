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
package optimus.platform.relational.tree

import optimus.core.utils.RuntimeMirror
import optimus.platform.relational.RelationalException

import scala.collection.immutable.{
  Map => MapAncestor
} // We need metadata field of MemberDescriptor be an immutable Map, while Map.empty be common parent of immutable and mutable Map
import scala.reflect.runtime.universe._

//////////////////////////////////////////////////////////////////////////
// Unify the Type system with a higher level abstraction
//////////////////////////////////////////////////////////////////////////

sealed abstract class ParameterDescriptor {
  val typeInfo: TypeInfo[_]
  val name: String
}

object MemberTypes extends Enumeration {
  type MemberTypes = Value

  val Constructor = Value(1)
  val Field = Value(4)
  val Method = Value(8)
}

sealed abstract class MemberDescriptor {
  val memberType: MemberTypes.MemberTypes
  val name: String
  val declaringType: TypeInfo[_]
  val metadata: Map[String, Any]

  override def equals(that: Any): Boolean = that match {
    case that: MemberDescriptor =>
      memberType == that.memberType && name == that.name && declaringType == that.declaringType
    case _ => false
  }

  override def hashCode: Int = (memberType.hashCode * 89 + name.hashCode) * 71 + declaringType.hashCode
}

sealed abstract class MethodDescriptor extends MemberDescriptor {
  val memberType = MemberTypes.Method
  val returnType: TypeInfo[_]
  val genericArgTypes: List[TypeInfo[_]]
  def getParameters(): List[ParameterDescriptor]
}

sealed abstract class ConstructorDescriptor extends MemberDescriptor {
  val memberType = MemberTypes.Constructor
  val name = "$lessinit$greater" // <init>
  def getParameters(): List[ParameterDescriptor]
}

sealed abstract class FieldDescriptor extends MemberDescriptor {
  val memberType = MemberTypes.Field
  val fieldType: TypeInfo[_]
}

object TypeDescriptor {
  val mirror = RuntimeMirror.forClass(getClass)

  def getMemberDescriptorWithMetadata(member: MemberDescriptor, meta: Map[String, Any]): MemberDescriptor = {
    member match {
      case field: RuntimeFieldDescriptor =>
        new RuntimeFieldDescriptor(field.declaringType, field.name, field.fieldType, meta)
      case method: RuntimeMethodDescriptor =>
        new RuntimeMethodDescriptor(
          method.reflectType,
          method.methodName,
          method.returnType,
          method.genericArgTypes,
          method.parametersMap,
          meta)
      case constructor: RuntimeConstructorDescriptor =>
        new RuntimeConstructorDescriptor(constructor.declaringType, meta)
      case _ =>
        throw new RuntimeException(
          "member descriptor " + member.memberType + " doesn't have corresponding match case to fill metadata")
    }
  }

  def getField(declaringType: TypeInfo[_], name: String): FieldDescriptor = {
    val fieldType = declaringType.propertyTypeErased(name).getOrElse(TypeInfo.ANY)
    new RuntimeFieldDescriptor(declaringType, name, fieldType)
  }

  private def matches(argumentTypes: List[Type], methodTypes: List[Type], compare: (Type, Type) => Boolean): Boolean =
    argumentTypes.length == methodTypes.length && !argumentTypes.zip(methodTypes).exists(t => !compare(t._1, t._2))

  def getGenericMethod(
      typeDescriptor: TypeInfo[_],
      name: String,
      genericTypeArguments: List[TypeInfo[_]],
      arguments: List[TypeInfo[_]]): MethodDescriptor = {
    val calleeClass = typeDescriptor.clazz
    val ms = calleeClass.getDeclaredMethods()
    if ("asInstanceOf".equals(name)) {
      new RuntimeMethodDescriptor(typeDescriptor, name, genericTypeArguments(0), genericTypeArguments)
    } else if (calleeClass.getDeclaredMethod(name) != null) {
      val resClz = calleeClass.getDeclaredMethod(name).getReturnType()
      new RuntimeMethodDescriptor(typeDescriptor, name, TypeInfo.generic(resClz), genericTypeArguments)
    } else throw new RelationalException("TypeInfo[_] " + typeDescriptor + " doesn't contain method " + name)

  }

  private def chooseTypeInfoBasedName(name: String): TypeInfo[_] = {
    name match {
      case "String"  => TypeInfo.STRING
      case "Int"     => TypeInfo.INT
      case "Long"    => TypeInfo.LONG
      case "Float"   => TypeInfo.FLOAT
      case "Double"  => TypeInfo.DOUBLE
      case "Short"   => TypeInfo.SHORT
      case "Byte"    => TypeInfo.BYTE
      case "Boolean" => TypeInfo.BOOLEAN
      case "Any"     => TypeInfo.ANY
      case _         => throw new RelationalException("type " + name + " is not a TypeInfo")
    }
  }

  def getTypeInfo(tree: Tree): TypeInfo[_] = {
    tree match {
      case Ident(name)             => chooseTypeInfoBasedName(name.decodedName.toString)
      case Select(qualifier, name) => chooseTypeInfoBasedName(name.decodedName.toString)
      case _ =>
        throw new RelationalException("tree " + tree.toString + " should not be type argument T of asInstanceOf[T]")
    }
  }
  def getMethod(typeDescriptor: TypeInfo[_], name: String, arguments: List[TypeInfo[_]]): MethodDescriptor = {
    val calleeClass = typeDescriptor.clazz
    if ("apply".equals(name)) new RuntimeMethodDescriptor(typeDescriptor, name, typeDescriptor)
    else if (
      "$plus".equals(name) || "$minus".equals(name) || "$times".equals(name) || "$div".equals(name) || "$percent"
        .equals(name)
    ) {
      if (
        arguments(0).clazz == classOf[Short] || arguments(0).clazz == classOf[Byte] || arguments(0).clazz == classOf[
          Char]
      )
        new RuntimeMethodDescriptor(typeDescriptor, name, typeDescriptor)
      else
        new RuntimeMethodDescriptor(typeDescriptor, name, arguments(0))
    } else if (
      "$eq$eq".equals(name)
      || "$bang$eq".equals(name)
      || "$greater".equals(name)
      || "$greater$eq".equals(name)
      || "$less".equals(name)
      || "$less$eq".equals(name)
    )
      new RuntimeMethodDescriptor(typeDescriptor, name, TypeInfo.BOOLEAN)
    else if (
      calleeClass != null && (typeDescriptor <:< Class.forName("optimus.platform.DynamicObject")) && calleeClass
        .getDeclaredMethod(name, classOf[String]) != null
    ) {
      val resClz = calleeClass.getDeclaredMethod(name, classOf[String]).getReturnType()
      new RuntimeMethodDescriptor(typeDescriptor, name, TypeInfo.javaTypeInfo(resClz))
    } else if (FunctionNames.IN.equals(name)) new RuntimeMethodDescriptor(typeDescriptor, name, arguments(0))
    else if (calleeClass != null && calleeClass.getDeclaredMethod(name) != null)
      new RuntimeMethodDescriptor(
        typeDescriptor,
        name,
        TypeInfo.generic(calleeClass.getDeclaredMethod(name).getReturnType()))
    else if (calleeClass == null)
      new RuntimeMethodDescriptor(typeDescriptor, name, TypeInfo.NOTHING)
    else
      throw new RelationalException("TypeInfo[_] " + typeDescriptor + " doesn't contain method " + name)

  }

  def getConstructor(typeDescriptor: TypeInfo[_], arguments: List[TypeInfo[_]]): ConstructorDescriptor =
    new RuntimeConstructorDescriptor(typeDescriptor)

  def getFunctionType(returnType: TypeInfo[_], parameters: TypeInfo[_]*): TypeInfo[_] = TypeInfo.NOTHING
}

sealed class RuntimeFieldDescriptor(
    val reflectType: TypeInfo[_],
    val name: String,
    val fieldType: TypeInfo[_],
    val metadata: Map[String, Any] = MapAncestor.empty)
    extends FieldDescriptor {
  val declaringType: TypeInfo[_] = reflectType
}

class MethodParameterDescriptor(val typeInfo: TypeInfo[_], val name: String) extends ParameterDescriptor

sealed class RuntimeMethodDescriptor(
    val reflectType: TypeInfo[_],
    val methodName: String,
    val returnType: TypeInfo[_],
    val genericArgTypes: List[TypeInfo[_]] = Nil,
    val parametersMap: List[(String, TypeInfo[_])] = Nil,
    val metadata: Map[String, Any] = MapAncestor.empty)
    extends MethodDescriptor {
  val name: String = methodName
  val declaringType: TypeInfo[_] = reflectType
  def getParameters(): List[ParameterDescriptor] = parametersMap map { case (name, typeInfo) =>
    new MethodParameterDescriptor(typeInfo, name)
  }
}

sealed class RuntimeConstructorDescriptor(
    val declaringType: TypeInfo[_],
    val metadata: Map[String, Any] = MapAncestor.empty)
    extends ConstructorDescriptor {
  def getParameters(): List[ParameterDescriptor] = Nil
}
