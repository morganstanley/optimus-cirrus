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
package optimus.platform.util

import java.lang.reflect.Modifier
import optimus.platform._
import optimus.platform.annotations.handle
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Entity

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

object ClassUtils {

  def classSymbol(cls: Class[_]): ClassSymbol = currentMirror.classSymbol(cls)
  def classToType(cls: Class[_]): Type = classSymbol(cls).toType
  def classToTypeInfo(cls: Class[_]): TypeInfo[_] = typeToTypeInfo(classToType(cls))
  def typeToClass(tp: Type): Class[_] = currentMirror.runtimeClass(tp)
  def typeToTypeInfo(tp: Type): TypeInfo[_] = {
    val runtimeTypes = tp match {
      case RefinedType(parents, _)                          => parents
      case t: TypeRef if !t.sym.isClass && t.sym.isAbstract => Seq(definitions.ObjectTpe)
      case t                                                => Seq(t)
    }
    val classes = runtimeTypes.map(t => currentMirror.runtimeClass(t))
    val typeParams = tp.typeArgs.map(typeToTypeInfo)
    TypeInfo(classes, Nil, Nil, typeParams)
  }

  def isJavaEnum(cls: Class[_]): Boolean = classOf[Enum[_]].isAssignableFrom(cls) || cls.isEnum
  def isScalaEnum(cls: Class[_]): Boolean = classOf[Enumeration#Value].isAssignableFrom(cls)
  def isEnumClass(cls: Class[_]): Boolean = isScalaEnum(cls) || isJavaEnum(cls)

  def isCollection(cls: Class[_]): Boolean = cls.isArray || classOf[Iterable[_]].isAssignableFrom(cls)

  def isOption(cls: Class[_]): Boolean = classOf[Option[_]].isAssignableFrom(cls) || isOptionSome(cls)

  def isOptionSome(cls: Class[_]): Boolean = classOf[Some[_]] == cls

  def isProduct(c: Class[_]): Boolean = classOf[scala.Product].isAssignableFrom(c)

  def isTuple(c: Product): Boolean = isTuple(c.getClass)
  def isTuple(c: Any): Boolean = c != null && isTuple(c.getClass)
  def isTuple(c: Class[_]): Boolean = c.getName.startsWith("scala.Tuple")
  def isTuple(tpe: Type): Boolean = tpe.typeSymbol.fullName.startsWith("scala.Tuple")

  def isAbstractClass(cls: Class[_]): Boolean = Modifier.isAbstract(cls.getModifiers)

  def isCaseClass(c: Any): Boolean = isCaseClass(c.getClass)
  def isCaseClass(c: Class[_]): Boolean = {
    val tpe = ReflectUtils.typeTagForClass(c).tpe.typeSymbol
    if (tpe.isClass) tpe.asClass.isCaseClass
    else false
  }

  def isCaseObject(c: Any): Boolean = isCaseObject(c.getClass)
  def isCaseObject(c: Class[_]): Boolean = {
    val tpe = ReflectUtils.typeTagForClass(c).tpe.typeSymbol
    if (tpe.isClass) tpe.asClass.isCaseClass && tpe.isModuleClass
    else false
  }

  def isSealedTrait(c: Any): Boolean = isSealedTrait(c.getClass)
  def isSealedTrait(c: Class[_]): Boolean = {
    val tpe = ReflectUtils.typeTagForClass(c).tpe.typeSymbol
    if (tpe.isClass) tpe.asClass.isTrait && tpe.asClass.isSealed
    else false
  }

  def isEntity(cls: Class[_]): Boolean = classOf[Entity].isAssignableFrom(cls)

  def isEntity(clsName: String): Boolean = {
    val cls = Class.forName(clsName)
    ClassUtils.isEntity(cls)
  }

  /**
   * Whether given Class is "Optimus Embeddable" or not. Implementation inspired by
   * [[optimus.dal.entitybrowser.util.ClassReflectionUtil.isEmbeddableCaseClass]].
   */
  def isEmbeddableCaseClass(cls: Class[_]): Boolean =
    if (!isEntity(cls) && isProduct(cls)) {
      val annotations = classToType(cls).typeSymbol.annotations
      annotations.exists(_.tree.tpe =:= weakTypeOf[embeddable])
    } else false

  def getClassMethods(
      cls: Class[_],
      sorted: Boolean = false
  ): Seq[MethodSymbol] = {
    val clsType = classToType(cls)
    val memberScope = if (sorted) clsType.members.sorted else clsType.members
    memberScope.collect { case m: MethodSymbol =>
      m
    }.toSeq
  }
}

object MethodUtils {
  implicit class RichMethodSymbol(method: MethodSymbol) {

    def decodedName: String = method.name.decodedName.toString

    /** Methods potentially having side effects. */
    def consideredSideEffecting: Boolean = {
      method.returnType.finalResultType.dealias =:= typeOf[Unit] ||
      method.annotations.exists { a =>
        val aType = a.tree.tpe
        aType =:= typeOf[async] || aType =:= typeOf[impure] || aType =:= typeOf[handle]
      }
    }

    def isNode: Boolean = method.annotations.exists(_.tree.tpe =:= weakTypeOf[node])

    /** Whether method has 'Unit' or 'Void' return type. */
    def hasVoidReturnType: Boolean = method.returnType =:= definitions.UnitTpe

    /** Whether method has an empty parameter list, e.g. 'def foo(): Unit' */
    def hasEmptyParameterList: Boolean = method.paramLists match {
      case Nil      => true
      case p :: Nil => p.isEmpty
      case _        => false
    }

    /** Whether method has no parameter list, e.g. 'def foo: Unit' */
    def isNullaryMethod: Boolean = method.info match {
      case NullaryMethodType(_) => true
      case _                    => false
    }

    /** Method either has no parameter or empty parameter list, e.g. 'def foo: Unit' or 'def foo(): Unit'. */
    def hasNoParameterList: Boolean = isNullaryMethod || hasEmptyParameterList
  }
}
