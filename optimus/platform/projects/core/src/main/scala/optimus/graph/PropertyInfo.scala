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
package optimus.graph

import optimus.entity._
import optimus.platform.pickling.Pickler
import optimus.platform.pickling.Registry
import optimus.platform.pickling.Unpickler
import optimus.platform.util.ReflectUtils
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Storable

import scala.annotation.StaticAnnotation
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/*
 * Token for serializing PropertyInfos.  PropertyInfos are essentially singletons but contained
 * as members within the companion object for entity classes.  We serialize the name and either the
 * jl.Class for @stored @entity classes, or the module itself for @stored @entity modules.
 *
 * Entity companions are not currently serializable, so we can't send them directly.  Instead,
 * find the companion using (cached) reflection via EntityInfoRegistry.
 *
 * TODO (OPTIMUS-0000): We need to support sending over @stored @entity modules in order to support distributing
 * node invocations on them, so we need to deal with the underlying issues around serializing
 * companions anyway.
 */
final case class PropertyInfoToken(name: String, klass: Class[_], isModule: Boolean) {
  @throws(classOf[java.io.ObjectStreamException])
  def readResolve(): AnyRef =
    if (isModule) {
      EntityInfoRegistry.getModuleInfo(klass).propertyMetadata(name)
    } else {
      EntityInfoRegistry.getInfo(klass).propertyMetadata(name)
    }
}

private object PropertyInfoUtils {
  val propertyInfoType: universe.Type = typeOf[PropertyInfo[_]]
  val propertyInfoSymbol: universe.Symbol = propertyInfoType.typeSymbol
}

/**
 * Root class for Property info (metadata for entity properties, independent of instance).
 * Only for @node def/val in @entity class. Do not new PropertyInfo() this probably will not work
 */
class PropertyInfo[R](_name: String, flags: Long = 0L, val annotations: collection.Seq[StaticAnnotation] = Nil)
    extends NodeTaskInfo(_name, flags, false /* Delayed registration */ )
    with Serializable {
  /*
   * Semantic changes here should be carried over to the rough Java counterpart (FieldMeta)
   * OPTIMUS-11628 covers the convergence of these two elements
   */
  type PropType = R

  final def setEntityInfo(entityInfo: OptimusInfo): Unit = {
    this.entityInfo = entityInfo
    this.profile = NodeTrace.registerWithLinkUp(this)
    if (needPicklers) {
      val typeTagOfR = this.typeTag
      this.pickler = Registry.picklerOf(typeTagOfR)
      this.unpickler = Registry.unpicklerOf(typeTagOfR)
    }
  }

  private def needPicklers: Boolean =
    isStored && (entityInfo match {
      case _: ModuleEntityInfo => false // even if it's storable, only the name is stored not the fields
      case entity: EntityInfo  => entity.isStorable
      case _: StorableInfo => true // only Events should get here (we can't match on EventInfo as it's in dal_client)
      case _               => false
    })

  // Forwarded for scala setter
  final def cacheable: Boolean = super.getCacheable
  final def cacheable_=(enable: Boolean): Unit = super.setCacheable(enable)

  /**
   * Compiler plugin generates the call to this setter
   */
  final def matchOn_=(lst: List[PropertyInfo[R]]): Unit = super.internalSetOn(lst.toArray)

  @throws(classOf[java.io.ObjectStreamException])
  def writeReplace(): AnyRef = entityInfo match {
    case mi: ModuleEntityInfo => PropertyInfoToken(name(), mi.runtimeClass, isModule = true)
    case ci: StorableInfo =>
      PropertyInfoToken(
        name(),
        ci.runtimeClass,
        isModule = false
      ) // NOTE: name() brackets are needed otherwise extra variable will be captured!
  }

  // TODO (OPTIMUS-0000): Do they really need to retain their types? If not they can be moved down to NodeTaskInfo
  var pickler: Pickler[R] = _
  var unpickler: Unpickler[R] = _

  // TODO (OPTIMUS-0000): deal with this in a uniform way (e.g. flag)
  def isChildToParent: Boolean = false

  private[optimus] final lazy val typeInfo: TypeInfo[R] = {
    val tt = typeTag
    def inner(tp: Type): TypeInfo[R] = {
      val runtimeTypes = tp match {
        case RefinedType(parents, _) => parents
        case t                       => Seq(t)
      }
      val erased = runtimeTypes.map(t => tt.mirror.runtimeClass(t.erasure))
      val tParams = tp.typeArgs.map(inner)
      new TypeInfo[R](erased, Nil, Nil, tParams)
    }

    inner(tt.tpe)
  }

  private[optimus] final lazy val typeTag: TypeTag[R] = {
    try {
      val entityTag = entityInfo.typeTag
      val mirror = entityTag.mirror
      val moduleTag = entityInfo.moduleTypeTag

      // if the entity is a module, the compiler plugin appends _info to the property names to avoid clashes
      val propInfoName = if (entityTag eq moduleTag) name + "_info" else name
      val propInfoTerm = moduleTag.tpe.decl(TermName(propInfoName)).asTerm

      // the simple case is that there's just one member with that name, in which case it must be the right one ...
      val propInfoVal =
        if (!propInfoTerm.isOverloaded) propInfoTerm
        else {
          // ... but there can be other methods with the same name as the property info, in which case we need to
          // find the one that's a val
          val possibilities = propInfoTerm.alternatives.collect { case m if m.isTerm => m.asTerm }.filter(_.isVal)
          assert(possibilities.size == 1, s"No unique val member: $possibilities")
          possibilities.head
        }

      // view as the PropertyInfo[_] base type, so that we know the only type argument is the property's return type
      val asPropertyInfoType = propInfoVal.info.baseType(PropertyInfoUtils.propertyInfoSymbol)
      ReflectUtils.mkTypeTag(asPropertyInfoType.typeArgs.head, mirror)
    } catch {
      case x: Exception =>
        val clazz = if (entityInfo != null) entityInfo.runtimeClass else null
        throw new GraphException(s"Failed to resolve return type for property $name on type $clazz", x)
    }
  }

  def runtimeClass(tp: Type): Class[_] = {
    val runtimeType = tp match {
      case RefinedType(parents, _) => parents.head
      case t                       => t
    }
    typeTag.mirror.runtimeClass(runtimeType.erasure)
  }

  def propertyType: Type = typeTag.tpe
  def propertyClass: Class[_] = runtimeClass(propertyType)
}

/**
 * Property info for vals, vars, and def0s (functions with no arguments).
 * TODO (OPTIMUS-0000): Consider using non-abstract PropertyInfo class for abstract members.
 */
abstract class DefPropertyInfo[E, WhenL, SetL, R](
    name: String,
    flags: Long,
    annotations: collection.Seq[StaticAnnotation] = Nil)
    extends PropertyInfo[R](name, flags, annotations)
    with ReflectionUtils {

  final protected def entityClassTag: ClassTag[E] = ClassTag(entityInfo.runtimeClass.asInstanceOf[Class[E]])
}

abstract class DefPropertyInfo0[E, WhenL, SetL, R](
    _name: String,
    flags: Long,
    annotations: collection.Seq[StaticAnnotation] = Nil)
    extends DefPropertyInfo[E, WhenL, SetL, R](_name, flags, annotations)
    with ReflectionUtils {
  private lazy val newNodeMethod = lookupMethod(entityInfo.runtimeClass, s"$name$$newNode")

  final def createNodeKey(e: E): NodeKey[R] = newNodeMethod.invoke(e).asInstanceOf[NodeKey[R]]
}

// These are generated for non-@node val entity properties.
class ReallyNontweakablePropertyInfo[E <: Storable, R](
    name: String,
    flags: Long,
    annotations: collection.Seq[StaticAnnotation] = Nil)
    extends DefPropertyInfo0[E, E => Boolean, E => R, R](name, flags, annotations)
    with ReflectionUtils {

  private lazy val getValueMethod = lookupMethod(entityInfo.runtimeClass, name)

  def getValue(e: E): R = getValueMethod.invoke(e).asInstanceOf[R]

}

class GenPropertyInfo(name: String, flags: Long, annotations: collection.Seq[StaticAnnotation] = Nil)
    extends PropertyInfo[Any](name, flags, annotations)

// Property info base classes. These are subclassed directly if property-level tweaks are not allowed
// (e.g. if a tweak handler is defined)
// These properties may still be tweakable on an instance level.  PropertyInfo.isTweakable will tell you this.
// TwkPropertyInfoN (below) extends these and adds Property-level tweak support.
class PropertyInfo0[E <: Storable, R](name: String, flags: Long, annotations: collection.Seq[StaticAnnotation] = Nil)
    extends DefPropertyInfo0[E, E => Boolean, E => R, R](name, flags, annotations)
class PropertyInfo1[E, P1, R](name: String, flags: Long)
    extends DefPropertyInfo[E, (E, P1) => Boolean, (E, P1) => R, R](name, flags)
class PropertyInfo2[E, P1, P2, R](name: String, flags: Long)
    extends DefPropertyInfo[E, (E, P1, P2) => Boolean, (E, P1, P2) => R, R](name, flags)
class PropertyInfo3[E, P1, P2, P3, R](name: String, flags: Long)
    extends DefPropertyInfo[E, (E, P1, P2, P3) => Boolean, (E, P1, P2, P3) => R, R](name, flags)
class PropertyInfo4[E, P1, P2, P3, P4, R](name: String, flags: Long)
    extends DefPropertyInfo[E, (E, P1, P2, P3, P4) => Boolean, (E, P1, P2, P3, P4) => R, R](name, flags)
class PropertyInfo5[E, P1, P2, P3, P4, P5, R](name: String, flags: Long)
    extends DefPropertyInfo[E, (E, P1, P2, P3, P4, P5) => Boolean, (E, P1, P2, P3, P4, P5) => R, R](name, flags)
class PropertyInfo6[E, P1, P2, P3, P4, P5, P6, R](name: String, flags: Long)
    extends DefPropertyInfo[E, (E, P1, P2, P3, P4, P5, P6) => Boolean, (E, P1, P2, P3, P4, P5, P6) => R, R](name, flags)
class PropertyInfo7[E, P1, P2, P3, P4, P5, P6, P7, R](name: String, flags: Long)
    extends DefPropertyInfo[E, (E, P1, P2, P3, P4, P5, P6, P7) => Boolean, (E, P1, P2, P3, P4, P5, P6, P7) => R, R](
      name,
      flags)
class PropertyInfo8[E, P1, P2, P3, P4, P5, P6, P7, P8, R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8) => R,
      R](name, flags)
class PropertyInfo9[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9) => R,
      R](name, flags)
class PropertyInfo10[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => R,
      R](name, flags)
class PropertyInfo11[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => R,
      R](name, flags)
class PropertyInfo12[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => R,
      R](name, flags)
class PropertyInfo13[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => R,
      R](name, flags)
class PropertyInfo14[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => R,
      R](name, flags)
class PropertyInfo15[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => R,
      R](name, flags)
class PropertyInfo16[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, R](
    name: String,
    flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => R,
      R](name, flags)
class PropertyInfo17[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, R](
    name: String,
    flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => R,
      R](name, flags)
class PropertyInfo18[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, R](
    name: String,
    flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => R,
      R](name, flags)
class PropertyInfo19[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, R](
    name: String,
    flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => R,
      R
    ](name, flags)
class PropertyInfo20[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, R](
    name: String,
    flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => R,
      R
    ](name, flags)
class PropertyInfo21[
    E,
    P1,
    P2,
    P3,
    P4,
    P5,
    P6,
    P7,
    P8,
    P9,
    P10,
    P11,
    P12,
    P13,
    P14,
    P15,
    P16,
    P17,
    P18,
    P19,
    P20,
    P21,
    R](name: String, flags: Long)
    extends DefPropertyInfo[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => R,
      R
    ](name, flags)

// Property-level tweakable propertyInfo classes.
class TwkPropertyInfo0[E <: Storable, R](name: String, flags: Long, annotations: collection.Seq[StaticAnnotation] = Nil)
    extends PropertyInfo0[E, R](name, flags, annotations)
    with PropertyTarget[E, E => Boolean, E => R, R]
class TwkPropertyInfo1[E, P1, R](name: String, flags: Long)
    extends PropertyInfo1[E, P1, R](name, flags)
    with PropertyTarget[E, (E, P1) => Boolean, (E, P1) => R, R]
class TwkPropertyInfo2[E, P1, P2, R](name: String, flags: Long)
    extends PropertyInfo2[E, P1, P2, R](name, flags)
    with PropertyTarget[E, (E, P1, P2) => Boolean, (E, P1, P2) => R, R]
class TwkPropertyInfo3[E, P1, P2, P3, R](name: String, flags: Long)
    extends PropertyInfo3[E, P1, P2, P3, R](name, flags)
    with PropertyTarget[E, (E, P1, P2, P3) => Boolean, (E, P1, P2, P3) => R, R]
class TwkPropertyInfo4[E, P1, P2, P3, P4, R](name: String, flags: Long)
    extends PropertyInfo4[E, P1, P2, P3, P4, R](name, flags)
    with PropertyTarget[E, (E, P1, P2, P3, P4) => Boolean, (E, P1, P2, P3, P4) => R, R]
class TwkPropertyInfo5[E, P1, P2, P3, P4, P5, R](name: String, flags: Long)
    extends PropertyInfo5[E, P1, P2, P3, P4, P5, R](name, flags)
    with PropertyTarget[E, (E, P1, P2, P3, P4, P5) => Boolean, (E, P1, P2, P3, P4, P5) => R, R]
class TwkPropertyInfo6[E, P1, P2, P3, P4, P5, P6, R](name: String, flags: Long)
    extends PropertyInfo6[E, P1, P2, P3, P4, P5, P6, R](name, flags)
    with PropertyTarget[E, (E, P1, P2, P3, P4, P5, P6) => Boolean, (E, P1, P2, P3, P4, P5, P6) => R, R]
class TwkPropertyInfo7[E, P1, P2, P3, P4, P5, P6, P7, R](name: String, flags: Long)
    extends PropertyInfo7[E, P1, P2, P3, P4, P5, P6, P7, R](name, flags)
    with PropertyTarget[E, (E, P1, P2, P3, P4, P5, P6, P7) => Boolean, (E, P1, P2, P3, P4, P5, P6, P7) => R, R]
class TwkPropertyInfo8[E, P1, P2, P3, P4, P5, P6, P7, P8, R](name: String, flags: Long)
    extends PropertyInfo8[E, P1, P2, P3, P4, P5, P6, P7, P8, R](name, flags)
    with PropertyTarget[E, (E, P1, P2, P3, P4, P5, P6, P7, P8) => Boolean, (E, P1, P2, P3, P4, P5, P6, P7, P8) => R, R]
class TwkPropertyInfo9[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, R](name: String, flags: Long)
    extends PropertyInfo9[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9) => R,
      R]
class TwkPropertyInfo10[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R](name: String, flags: Long)
    extends PropertyInfo10[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => R,
      R]
class TwkPropertyInfo11[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R](name: String, flags: Long)
    extends PropertyInfo11[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => R,
      R]
class TwkPropertyInfo12[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, R](name: String, flags: Long)
    extends PropertyInfo12[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => R,
      R]
class TwkPropertyInfo13[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, R](name: String, flags: Long)
    extends PropertyInfo13[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => R,
      R]
class TwkPropertyInfo14[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, R](name: String, flags: Long)
    extends PropertyInfo14[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => R,
      R]
class TwkPropertyInfo15[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, R](
    name: String,
    flags: Long)
    extends PropertyInfo15[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => R,
      R]
class TwkPropertyInfo16[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, R](
    name: String,
    flags: Long)
    extends PropertyInfo16[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => R,
      R]
class TwkPropertyInfo17[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, R](
    name: String,
    flags: Long)
    extends PropertyInfo17[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, R](
      name,
      flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => R,
      R]
class TwkPropertyInfo18[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, R](
    name: String,
    flags: Long)
    extends PropertyInfo18[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, R](
      name,
      flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => R,
      R]
class TwkPropertyInfo19[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, R](
    name: String,
    flags: Long)
    extends PropertyInfo19[E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, R](
      name,
      flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => R,
      R
    ]
class TwkPropertyInfo20[
    E,
    P1,
    P2,
    P3,
    P4,
    P5,
    P6,
    P7,
    P8,
    P9,
    P10,
    P11,
    P12,
    P13,
    P14,
    P15,
    P16,
    P17,
    P18,
    P19,
    P20,
    R](name: String, flags: Long)
    extends PropertyInfo20[
      E,
      P1,
      P2,
      P3,
      P4,
      P5,
      P6,
      P7,
      P8,
      P9,
      P10,
      P11,
      P12,
      P13,
      P14,
      P15,
      P16,
      P17,
      P18,
      P19,
      P20,
      R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => R,
      R
    ]
class TwkPropertyInfo21[
    E,
    P1,
    P2,
    P3,
    P4,
    P5,
    P6,
    P7,
    P8,
    P9,
    P10,
    P11,
    P12,
    P13,
    P14,
    P15,
    P16,
    P17,
    P18,
    P19,
    P20,
    P21,
    R](name: String, flags: Long)
    extends PropertyInfo21[
      E,
      P1,
      P2,
      P3,
      P4,
      P5,
      P6,
      P7,
      P8,
      P9,
      P10,
      P11,
      P12,
      P13,
      P14,
      P15,
      P16,
      P17,
      P18,
      P19,
      P20,
      P21,
      R](name, flags)
    with PropertyTarget[
      E,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => Boolean,
      (E, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => R,
      R
    ]
