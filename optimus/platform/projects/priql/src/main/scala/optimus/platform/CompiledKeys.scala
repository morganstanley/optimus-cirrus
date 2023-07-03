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
package optimus.platform

import msjava.slf4jutils.scalalog._
import optimus.graph.{AlreadyCompletedNode, Node}
import optimus.platform._
import optimus.platform.annotations._
import optimus.platform.CompiledKey.NodeOrFunc
import optimus.platform.relational.asm.ASMKeyFactory
import optimus.platform.relational.internal.ComputingCache
import optimus.platform.relational.tree
import optimus.platform.relational.tree._
import optimus.platform.util.ScalaCompilerUtils

import scala.collection.GenTraversable
import scala.collection.compat._

object CompiledKey {
  type NodeOrFunc = Either[Any => Node[AnyRef], Any => AnyRef]
}

abstract class CompiledKey[-K: TypeInfo](factory: NodeOrFunc) extends RelationKey[K] {
  @nodeSync
  override def of(t: K): AnyRef = {
    val n = of$queued(t)
    n.get
  }
  override def of$queued(t: K): Node[AnyRef] = {
    if (t == null) new AlreadyCompletedNode(null)
    else
      factory match {
        case Left(node) => node(t).enqueue
        case Right(fn)  => new AlreadyCompletedNode(fn(t))
      }
  }

  def of$newNode(t: K): Node[AnyRef] = {
    if (t == null) new AlreadyCompletedNode(null)
    else
      factory match {
        case Left(node) => node(t)
        case Right(fn)  => new AlreadyCompletedNode(fn(t))
      }
  }

  override def isSyncSafe: Boolean = {
    factory.isRight
  }

  private[this] val syncFactory = factory.right.getOrElse(null)

  override def ofSync(t: K): AnyRef = {
    syncFactory(t)
  }

  override lazy val isKeyComparable: Boolean = {
    RelationKey.isFieldsComparable(tree.typeInfo[K], fields)
  }

  override def compareKeys(leftKey: Any, rightKey: Any): Int = {
    (leftKey, rightKey) match {
      case (null, null)                       => 0
      case (_, null)                          => 1
      case (null, _)                          => -1
      case (l: Comparable[Any] @unchecked, r) => l.compareTo(r)
      case (l, _) => throw new IllegalArgumentException(s"type ${l.getClass} is not Comparable")
    }
  }
}

// makes it easier to write companions for Keys which take a single type tag
trait SingleTypeCompiledKeyFactory {
  type KeyType[_]

  val log = getLogger[SingleTypeCompiledKeyFactory]

  private val cache = new ComputingCache[TypeInfo[_], KeyType[_]]({ tpeInfo =>
    val flds = fields(tpeInfo)
    val factory = createFactory(tpeInfo, flds)
    create(factory, flds)(tpeInfo)
  })

  private[optimus /*platform*/ ] def clearCache(): Unit = { cache.clear() }

  protected def create[X: TypeInfo](factory: NodeOrFunc, fields: Seq[String]): KeyType[X]

  protected def fields(t: TypeInfo[_]): Seq[String]

  protected def keyDescription(t: TypeInfo[_]): String

  protected def createFactory(tpeInfo: TypeInfo[_], fields: Seq[String]): NodeOrFunc =
    ASMKeyFactory(tpeInfo, fields)

  def precompile(tags: Seq[TypeInfo[_]]): Unit = {
    // n.b. we now compile one-by-one rather than bulk compiling. although bulk is faster, it causes
    // the compiler lock to be held for longer which prevents other threads from compiling keys
    // which might be needed first.
    for (tag <- tags) apply(tag)
  }

  def apply[K: TypeInfo]: KeyType[K] = {
    cache(typeInfo[K]).asInstanceOf[KeyType[K]]
  }
}

class AutoKey[K: TypeInfo] private[platform] (factory: NodeOrFunc, val fields: Seq[String])
    extends CompiledKey[K](factory) {
  def typeInfo: TypeInfo[K] = tree.typeInfo[K]
}

object AutoKey extends SingleTypeCompiledKeyFactory {
  type KeyType[X] = AutoKey[X]

  def typeInfo[T: TypeInfo]: TypeInfo[AutoKey[T]] = tree.typeInfo[AutoKey[T]]

  protected[platform] override def fields(inputGenericTypeInfo: TypeInfo[_]): Seq[String] =
    inputGenericTypeInfo.propertyNames.toSeq
  protected override def create[X: TypeInfo](factory: NodeOrFunc, fields: Seq[String]): AutoKey[X] =
    new AutoKey[X](factory, fields)
  protected override def keyDescription(t: TypeInfo[_]): String = s"AutoKey[${t.name}]"
}

/**
 * a key which includes all fields in T except those in M
 */
final class ModuloKey[T: TypeInfo, M >: T: TypeInfo] private (factory: NodeOrFunc, val fields: Seq[String])
    extends CompiledKey[T](factory) {
  def moduloTypeInfo: TypeInfo[M] = {
    tree.typeInfo[M]
  }

  override def hashCode: Int = {
    fields.hashCode
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case m: ModuloKey[_, _] => fields == m.fields
      case _                  => false
    }
  }
}

object ModuloKey {
  // these keys depend on two types, but we'd like to re-use the functionality of SingleTypeCompiledKeyFactory, so let's
  // have an map of modulo type to factory... (we're basically currying the factory)
  private val moduloTypeToFactoryCache = new ComputingCache[TypeInfo[_], ModuloKeyFactory[_]]({ tag =>
    new ModuloKeyFactory()(tag)
  })

  private class ModuloKeyFactory[M](implicit moduloTypeTag: TypeInfo[M]) extends SingleTypeCompiledKeyFactory {
    type KeyType[_] = ModuloKey[_, M]

    val moduleTypeInfo = moduloTypeTag
    val moduloFields = AutoKey.fields(moduleTypeInfo).toSet

    override protected def fields(inputType: TypeInfo[_]): Seq[String] =
      AutoKey.fields(inputType).filterNot(moduloFields)
    override protected def keyDescription(t: TypeInfo[_]): String = s"ModuloKey[${t.name}, ${moduloTypeTag.name}]"
    override protected def create[T: TypeInfo](factory: NodeOrFunc, fields: Seq[String]): ModuloKey[Nothing, M] =
      new ModuloKey[Nothing, M](factory, fields)(tree.typeInfo[T].cast[Nothing], moduloTypeTag)
  }

  private def factoryForModuloType[M](moduloType: TypeInfo[M]): ModuloKeyFactory[M] =
    moduloTypeToFactoryCache.synchronized {
      moduloTypeToFactoryCache(moduloType).asInstanceOf[ModuloKeyFactory[M]]
    }

  def precompile(tags: Seq[(TypeInfo[_], TypeInfo[_])]): Unit = {
    val groupedByModulo = tags.groupMap(_._2)(x => x._1)

    for ((moduloType, inputTypes) <- groupedByModulo) {
      val factory = factoryForModuloType(moduloType)
      factory.precompile(inputTypes)
    }
  }

  def apply[T, M >: T](implicit t: TypeInfo[T], m: TypeInfo[M]): ModuloKey[T, M] = {
    factoryForModuloType[M](m).apply[T](t).asInstanceOf[ModuloKey[T, M]]
  }

  private[platform] def clearCache(): Unit = { moduloTypeToFactoryCache.clear() }

  def typeInfo[T: TypeInfo, M >: T: TypeInfo]: TypeInfo[ModuloKey[T, M]] = tree.typeInfo[ModuloKey[T, M]]
}

/**
 * The SuperKey type builds a key from the full set of attributes of the underlying type.
 */
class SuperKey[T: TypeInfo] private (factory: NodeOrFunc, val fields: Seq[String]) extends CompiledKey[T](factory) {
  override lazy val isKeyComparable: Boolean = {
    val tInfo = typeInfo[T]
    if (SuperKey.isBuiltIn(tInfo)) {
      RelationKey.isTypeComparable(tInfo)
    } else RelationKey.isFieldsComparable(tInfo, fields)
  }
}

object SuperKey extends SingleTypeCompiledKeyFactory {
  type KeyType[X] = SuperKey[X]

  def typeInfo[X: TypeInfo]: TypeInfo[SuperKey[X]] = tree.typeInfo[SuperKey[X]]

  // The benefit is that it works for primitives, strings, tuples etc.
  // which don't necessarily have suitable fields to extract.
  def isBuiltIn(cls: Class[_]): Boolean =
    cls.getName.startsWith("java") || cls.getName.startsWith("scala") ||
      ScalaCompilerUtils.isPrimitive(cls) || classOf[GenTraversable[_]].isAssignableFrom(cls)

  def isBuiltIn(s: TypeInfo[_]): Boolean = (s.concreteClass ++ s.interfaces).forall(isBuiltIn)

  override protected def fields(inputGenericTypeInfo: TypeInfo[_]): Seq[String] = AutoKey.fields(inputGenericTypeInfo)
  override protected def create[X: TypeInfo](factory: NodeOrFunc, fields: Seq[String]): SuperKey[X] =
    new SuperKey[X](factory, fields)
  override protected def createFactory(tpe: TypeInfo[_], fields: Seq[String]): NodeOrFunc = {
    // we'll use identity function for concrete built-in types, because our field extraction for these
    // often doesn't work (e.g. for Tuple or Int or Seq). otherwise we'll use the compiled key for all fields
    if (isBuiltIn(tpe) || fields == null || fields.isEmpty) Right((x: Any) => x.asInstanceOf[AnyRef])
    else super.createFactory(tpe, fields)
  }

  override protected def keyDescription(t: TypeInfo[_]) = s"SuperKey[${t.name}]"

}

class DimensionKey[T: TypeInfo] private (factory: NodeOrFunc, val fields: Seq[String]) extends CompiledKey[T](factory) {
  def typeInfo: TypeInfo[T] = tree.typeInfo[T]
}

object DimensionKey extends SingleTypeCompiledKeyFactory {
  type KeyType[X] = RelationKey[X]
  def typeInfo[T: TypeInfo]: TypeInfo[DimensionKey[T]] = tree.typeInfo[DimensionKey[T]]

  protected[platform] override def fields(inputGenericTypeInfo: TypeInfo[_]): Seq[String] = {
    inputGenericTypeInfo.dimensionPropertyNames
  }

  protected override def create[T: TypeInfo](factory: NodeOrFunc, fields: Seq[String]): RelationKey[T] = {
    if (fields.isEmpty) NoKey else new DimensionKey[T](factory, fields)
  }
  protected override def keyDescription(t: TypeInfo[_]): String = {
    s"DimensionKey[${t.name}]"
  }
}
