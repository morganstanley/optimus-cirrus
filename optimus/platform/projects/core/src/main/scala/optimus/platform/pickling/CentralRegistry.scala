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
package optimus.platform.pickling

import java.util.concurrent.ConcurrentHashMap
import optimus.platform.util.RuntimeServicesLoader

import scala.reflect.runtime.universe._
import PicklingReflectionUtils._
import optimus.core.config.StaticConfig

/**
 * CentralRegistry for values of type Pickler.
 */
private[pickling] class PicklerRegistry extends RuntimeServicesLoader[PicklerFactory] with CentralRegistry[Pickler] {
  protected val factories: List[Factory[Pickler]] = {
    // We no longer support CustomPickling through the factory approach. They must be defined with the
    // @embeddable(customPickling=...) construct
    Class
      .forName("optimus.platform.pickling.DefaultPicklerFactory")
      .getConstructor()
      .newInstance()
      .asInstanceOf[Factory[Pickler]] :: Nil
  }

  protected val hintMsg: String = "Have you forgotten any Optimus annotation, such as @embeddable and @entity? " +
    "If not, rather than defining custom picklers, have you considered using transformers instead?"
}

/**
 * CentralRegistry for values of type Unpickler
 */
private[pickling] class UnpicklerRegistry
    extends RuntimeServicesLoader[UnpicklerFactory]
    with CentralRegistry[Unpickler] {
  protected val factories: List[Factory[Unpickler]] = {
    // We no longer support CustomPickling through the factory approach. They must be defined with the
    // @embeddable(customPickling=...) construct
    Class
      .forName("optimus.platform.pickling.DefaultUnpicklerFactory")
      .getConstructor()
      .newInstance()
      .asInstanceOf[Factory[Unpickler]] :: Nil
  }

  protected val hintMsg: String = "Have you forgotten any Optimus annotation, such as @embeddable and @entity? " +
    "If not, rather than defining custom unpicklers, have you considered using transformers instead?"
}

/**
 * CentralRegistry for values of type Ordering.
 *
 * This is needed because we need an instance of Ordering to create some unpicklers (e.g.: Unpickler[SortedSet])
 */
private[pickling] class OrderingRegistry extends RuntimeServicesLoader[OrderingFactory] with CentralRegistry[Ordering] {
  protected val factories: List[Factory[Ordering]] = services

  protected val hintMsg: String = "You may need to register your custom ordering using an OrderingFactory instance."
}

/**
 * A CentralRegistry provides an instance for a given type T.
 *
 * A CentralRegistry works as follows:
 *   - It checks if a value is in the cache
 *   - If present it returns it. If missing, it delegates its generation to a list of factories. The factories are
 *     requested to apply rules in the following order: 1) by considering its class (high priority rule) 2) by
 *     considering its companion superclasses (low priority rule) 3) by considering its class superclasses (low priority
 *     rule) The low priority rules are applied only if no value could be generated using the high priority ones.
 *
 *   - After delegating to the factories, it selects exactly one generated value, caches it and returns it.
 *
 * If the factories are unable to generate exactly one value (either because no value or multiple values are generated),
 * an exception is thrown.
 *
 * @tparam Value,
 *   the type of values it needs to provide
 */
trait CentralRegistry[Value[_]] {

  protected def factories: List[Factory[Value]]
  protected def hintMsg: String
  protected def hint: String = {
    val docStr = {
      val loadDoc = StaticConfig.string("customPicklerDocs")
      if (loadDoc.nonEmpty) s" For more information, see $loadDoc"
      else ""
    }
    hintMsg + docStr
  }

  private val cache: ConcurrentHashMap[Type, LazyWrapper[Value[_]]] = new ConcurrentHashMap()

  final def lazyInstanceFor[T: TypeTag]: LazyWrapper[Value[T]] = {
    val tpe = typeOf[T]
    lazyInstanceFor(tpe).asInstanceOf[LazyWrapper[Value[T]]]
  }

  // NOTE: Use only for classes with no type parameters!
  final def unsafeLazyInstanceFor[T](clazz: Class[T]): LazyWrapper[Value[T]] = {
    val tpe = classToType(clazz)
    lazyInstanceFor(tpe).asInstanceOf[LazyWrapper[Value[T]]]
  }

  final def lazyInstanceFor(tpe: Type): LazyWrapper[Value[_]] = lazyInstanceFor(tpe, _ => None)

  final def lazyInstanceFor(tpe: Type, preResolve: Type => Option[Value[_]]): LazyWrapper[Value[_]] =
    cache.computeIfAbsent(keyOf(tpe), t => new LazyWrapper[Value[_]](preResolve(tpe).getOrElse(generate(t))))

  private def keyOf(tpe: Type): Type = tpeDeepDealias(tpe)

  private def generate(tpe: Type): Value[_] = {
    val generatedValues: List[Value[_]] = {
      val highPriorityValues = applyHighPriorityRules(tpe)
      if (highPriorityValues.nonEmpty) highPriorityValues
      else applyLowPriorityRules(tpe)
    }
    generatedValues match {
      case List(valueOfT) => valueOfT
      case valuesOfT =>
        throw new IllegalStateException(
          s"Registry $getClass: expecting exactly one value generated for $tpe, but found ${valuesOfT.size}. HINT: $hint")
    }
  }

  private def applyHighPriorityRules(tpe: Type): List[Value[_]] = {
    val classKey = extractName(tpe)
    factories.flatMap(_.createInstanceFromClass(tpe, classKey))
  }

  private def applyLowPriorityRules(tpe: Type): List[Value[_]] = {
    val superclasses = getSuperclassKeys(tpe)
    factories.flatMap { factory =>
      factory.createInstanceFromSuperclass(tpe, superclasses)
    }
  }

  private def getSuperclassKeys(tpe: Type): List[String] = baseClasses(tpe).map(extractName)
}

// The cache is a ConcurrentHashMap populated using `computeIfAbsent`, which requires computations to be
// "short and simple" (see its documentation). For this reason, first we populate the map with lazy references
// (which are quick to create and lightweight): this should avoid contention in the map.
// The lazy references will be resolved later on, when calling Registry.picklerOf/unpicklerOf:
// this allows us to deal with recursive data structures
final class LazyWrapper[T](wrp: => T) {

  // using a lazy val is too aggressive as it causes many blocked threads,
  // we do something a bit more relaxed instead
  @volatile private var cachedT: Option[T] = None

  def eval: T = cachedT match {
    case Some(t) => t
    case None    =>
      // evaluating and caching the value
      val t = wrp
      cachedT = Option(t) // using Option(_) rather than Some(_) so that we do not cache null t values
      t
  }
}

object LazyWrapper {
  implicit def unboxLazy[T](wrapper: LazyWrapper[T]): T = wrapper.eval
}
