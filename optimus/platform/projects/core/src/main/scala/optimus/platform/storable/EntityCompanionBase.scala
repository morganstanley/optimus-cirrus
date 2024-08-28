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
package optimus.platform.storable

import optimus.core.utils.RuntimeMirror
import optimus.entity._
import optimus.platform.node
import optimus.platform.pickling.PicklingException
import optimus.platform.relational.tree.MemberDescriptor
import optimus.platform.util.ClientEmbeddableHierarchy

import scala.reflect.runtime.JavaUniverse
import scala.reflect.runtime.{universe => ru}
import optimus.utils.CollectionUtils._

trait StorableCompanionBase[T <: Storable] {
  def info: StorableInfo
}

trait EntityCompanionBase[T <: Entity] extends StorableCompanionBase[T] {
  def info: ClassEntityInfo
}
// have a look at KeyedEntityCompanionBase for methods specifically on Entities with Keys
object EmbeddableCompanionBase {

  private[optimus] def primaryConstructorInfoFor(clazz: Class[_ <: EmbeddableCompanionBase]) = {
    val jru = ru.asInstanceOf[JavaUniverse]
    val mirror = RuntimeMirror.forClass(clazz)
    val myCompanionClass = mirror.classSymbol(clazz).companion
    val constructors = myCompanionClass.typeSignature.decl(ru.termNames.CONSTRUCTOR).alternatives
    val primaryConstructor = constructors.find(ctr => ctr.asMethod.isPrimaryConstructor).getOrElse {
      throw new IllegalStateException(s"Unable to find primary constructor for class ${clazz.getCanonicalName}.")
    }
    val javaConstructor = mirror
      .asInstanceOf[jru.JavaMirror]
      .constructorToJava(primaryConstructor.asMethod.asInstanceOf[jru.MethodSymbol])
    val hasNodeAnnotation = javaConstructor.getAnnotation(classOf[node]) != null
    javaConstructor -> hasNodeAnnotation
  }
}

/** Companion objects of @embeddable case classes extend this (due to AdjustAST) */
trait EmbeddableCompanionBase {

  private[optimus] final lazy val primaryConstructorInfo = EmbeddableCompanionBase.primaryConstructorInfoFor(getClass)

  final lazy val shapeName = {
    val mirror = RuntimeMirror.forClass(getClass)
    val myCompanionClass = mirror.classSymbol(getClass).companion
    myCompanionClass.fullName
  }

  def fromArray(a: Array[AnyRef]): Embeddable = {
    val (constructor, isConstructorNode) = primaryConstructorInfo
    val t = constructor.newInstance(a: _*)
    // use constructor node if available, else use call constructor directly
    val cachedT = if (isConstructorNode) NodeSupport.lookupConstructorCache(t) else t
    cachedT.asInstanceOf[Embeddable]
  }

  def toArray(e: Embeddable): Array[AnyRef] = {
    val p = e.asInstanceOf[Product]
    val len = p.productArity
    val arr = new Array[AnyRef](len)
    var i = 0
    while (i < len) {
      arr(i) = p.productElement(i).asInstanceOf[AnyRef]
      i += 1
    }
    arr
  }

  def projectedMembers: List[MemberDescriptor] = Nil
}

/** Companion objects of @embeddable traits extend this trait (due to AdjustAST) */
trait EmbeddableTraitCompanionBase {

  /** all transitive concrete subtypes of this @embeddable trait from the current classpath, keyed by class simple name */
  @transient final lazy val subtypeSimpleNameToClass: Map[String, Class[Embeddable]] = {
    val traitName = getClass.getName.stripSuffix("$")
    val meta = ClientEmbeddableHierarchy.hierarchy.metaData(traitName)
    meta.allChildren
      .map(m => Class.forName(m.fullClassName).asInstanceOf[Class[Embeddable]])
      // interfaces (traits) can't be directly instantiated so we'll never be pickling an instance of them, and so it's
      // allowed for their simplenames to conflict with @embeddable object/class names, so we must remove them before
      // checking for conflicts
      .filterNot(_.isInterface)
      // note that objects are referred to by their dollarless simple name
      .groupBy(_.getSimpleName.stripSuffix("$"))
      .map { case (simpleName, classes) =>
        val cls = classes.singleOr {
          val names = classes.map(_.getName).toSeq.sorted.mkString(" and ")
          throw new PicklingException(
            s"@embeddable trait $traitName has subtypes (classes/objects/traits) " +
              s"with conflicting simple name '$simpleName': $names")
        }
        (simpleName, cls)
      }
  }
}

/**
 * Make companion object of @embeddable trait extend below trait to have default unpickleable value behavior. In
 * other words, default value will be set if the corresponding definition of embeddable class/object stored in the
 * entity isn't available in the current version of code.
 *
 * Example usage:
 * {{{
 *   @embeddable sealed trait Foo
 *   object Foo extends HasDefaultUnpickleableValue[Foo] {
 *     @embeddable case object defaultUnpickleableValue extends Foo
 *   }
 *   @embeddable case object FooEnum1 extends Foo
 *   @embeddable case object FooEnum2 extends Foo
 * }}}
 */
trait HasDefaultUnpickleableValue[T] { _: EmbeddableTraitCompanionBase =>
  def defaultUnpickleableValue: T
}
