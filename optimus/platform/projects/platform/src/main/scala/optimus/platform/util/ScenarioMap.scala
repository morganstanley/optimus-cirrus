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
import optimus.core.needsPlugin
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs
import optimus.platform.annotations.withLocationTag
import optimus.platform.util.scenariomap.AsyncIndex
import optimus.platform.util.scenariomap.Builder
import optimus.platform.util.scenariomap.Index
import optimus.platform.util.scenariomap.IndexMap
import optimus.platform.util.scenariomap.Parameters
import optimus.platform.util.scenariomap.Store
import optimus.platform.util.scenariomap.SyncIndex
import optimus.ui.HandlerResult
import optimus.ui.Immediately

import scala.collection.compat._

object ScenarioMap {
  def uniqueInstance[K, V]: ScenarioMap[K, V] = builder[K, V].build

  @withLocationTag
  def apply[K, V](): ScenarioMap[K, V] = needsPlugin
  def apply$LT[K, V](tag: LocationTag): ScenarioMap[K, V] = builder[K, V].build(tag)

  /**
   * Builder for creating new ScenarioMap with more control over parameters and allowing for additional indices.
   */
  def builder[K, V]: Builder[K, V] = Builder.empty

  /**
   * Create an auxiliary index that can be used to obtain an automatically updated view of a scenario map.
   */
  def index[K, K1](f: K => K1): Index[K, K1] = SyncIndex(f)
  def index$NF[K, K1](f: K => K1 @node): Index[K, K1] = AsyncIndex(f)

  // Only "materialize" does actual work outside of tweak resolution.
  ScenarioMap.get.setCacheable(false)
  ScenarioMap.keySet.setCacheable(false)
}

/**
 * ==Overview==
 * A map-like collection encoded in the scenario using tweaks.
 *
 * ScenarioMap is setup to reduce as much as possible looking at any tweaks that aren't needed. This is a powerful
 * optimization specifically in UI applications that rely on invalidations for rendering. Consider (for example) adding
 * a value to a normal map on a tweak x,
 *
 * {{{
 *   @node(tweak = true) def x: Map[Int, String] = Map.empty
 *   @handle def putIntoMap(i: Int, str: String) = Tweaks.byValue(x := x + (i -> str))
 *   @node def getThree = x.get(3)
 * }}}
 *
 * All calls to x (and their callers) will be invalidated whenever a value is changed in putIntoMap. This includes
 * `getThree`, even if the updated value isn't 3. By comparison, the following will not invalidate `getThree`
 * unnecessarily,
 *
 * {{{
 *   @node(tweak = true) val x: ScenarioMap[Int, String] = ScenarioMap.empty
 *   @handle def putIntoMap(i: Int, str: String) = x.updater.put(i, str).toHandlerResult
 *   @node def getThree = x.get(3)
 * }}}
 *
 * `x.get(3)` will only cause an invalidation when the value for the key 3 is itself changed. This is possible because
 * [[ScenarioMap]] is entirely constructed inside the Optimus scenario.
 *
 * Updates to a [[ScenarioMap]] are made using `.updater` which returns a builder-style [[ScenarioMapUpdater]] for the
 * [[ScenarioMap]] instance. The builder needs to be converted into a [[HandlerResult]] for its change to be reflected
 * into the scenario.
 *
 * ScenarioMap is meant as a drop-in replacement for TweakableMap / TweakableBackedMap.
 *
 * ==Indices==
 * [[ScenarioMap]] provides an API to get automatically updated alternate indices, which must be built at the same time
 * as the [[ScenarioMap]] instance. For example,
 *
 * {{{
 *   val (map, bySize, byFirstElement) = {
 *     import ScenarioMap._
 *     val index1 = index((s: Seq[Int]) => s.size)
 *     val index2 = index((s: Seq[Int]) => s.headOption)
 *     val entity = builder[Seq[Int], Int]
 *       .withIndex(index1)
 *       .withIndex(index2)
 *       .build
 *
 *     // Note! calling indexed without withIndex will throw
 *     (entity, entity.indexed(index1), entity.indexed(index2))
 *   }
 * }}}
 *
 * produces a `ScenarioMap[Seq[Int], Int]` and two automatically updated views from the `size` or `headOption` of each
 * key. This makes it easier to create high-performance alternative groupings of a given scenario map. Indices that are
 * node functions *must* be scenario independent.
 *
 * Note that alternate indices will be updated and materialized in the scenario whenever the set of keys in the map
 * changes even if they aren't actually used.
 */
@entity class ScenarioMap[K, V](tag: LocationTag, params: Parameters[K]) extends ScenarioMapImpl[K, V](tag, params) {

  /**
   * Get the value associated with a specific key.
   *
   * This call will be invalidated only if the key's value changes.
   */
  @node def get(k: K): Option[V] = store.get(k)

  /**
   * Create an updater for this map. See [[ScenarioMapUpdater]] for details. Note that updates will only be applied
   * as part of the `HandleResult` returned by the updater
   * @return
   */
  def updater: ScenarioMapUpdater[K, V] = emptyUpdater

  /**
   * Transforms the [[ScenarioMap]] into a standard immutable Scala map. This is an expensive method that will create
   * invalidations on every update of the [[ScenarioMap]] and should be avoided whenever possible.
   */
  @node def materialize: Map[K, V] = store.transformIntoMap

  /**
   * Keys that this map has in the current scenario, as a standard Scala immutable Set.
   *
   * This call will be invalidated whenever the key set changes (that is, when keys are added or removed), but not when
   * values are updated.
   */
  @node def keySet: Set[K] = store.keySet

  /**
   * Return a view that will remain in sync and remain materialized based on an index. The index must have been one that
   * was passed to [[ScenarioMap.builder]].
   */
  def indexed[K1](index: Index[K, K1]): ScenarioMapView[K1, Set[V]] = indexFor(index)
}

/**
 * View for [[ScenarioMap]]
 *
 * A view provides a way to do various operations on a [[ScenarioMap]] without having to materialize it into a real
 * [[Map]]. Creating a view doesn't cause tracking for invalidations, but using the view will.
 *
 * Views are stateless: they reflect changes to the parent [[ScenarioMap]] that are encoded in the current scenario.
 * This means that they can be shared or stored without any issues, so long as they are still used within the correct
 * scenarios.
 */
@entity abstract class ScenarioMapView[K, V] {
  @node def get(k: K): Option[V]
  @node def keySet: Set[K]

  /**
   * Create a view that lazily maps values to a new type.
   */
  @alwaysAutoAsyncArgs
  def mapValues[V1](f: V => V1): ScenarioMapView[K, V1] = ???
  def mapValues[V1](f: V => V1 @node): ScenarioMapView[K, V1] = Views.MapValues(this, f)

  /**
   * Create a view that has filtered keys. The view `.get()` will only be invalidated when a specific key changes and
   * that key isn't filtered.
   */
  @alwaysAutoAsyncArgs
  def filterKeys(f: K => Boolean): ScenarioMapView[K, V] = ???
  def filterKeys(f: K => Boolean @node): ScenarioMapView[K, V] = Views.FilterKeys(this, f)

  /**
   * Create a view that aggregates keys differently.
   *
   * The view `.get()` method will be invalidated whenever the key set on the parent view changes -- that is, when
   * elements are added or removed, but not when they are updated.
   *
   * Indices provided a similar mechanism with fewer invalidations, at the cost of more expensive updates.
   */
  @alwaysAutoAsyncArgs
  def groupBy[K1](f: K => K1): ScenarioMapView[K1, Set[V]] = ???
  def groupBy[K1](f: K => K1 @node): ScenarioMapView[K1, Set[V]] = Views.GroupBy(this, f)
}

/**
 * Marker trait for views that can be Materialized by an updater.
 */
sealed trait Materializable {
  type Data
  @async def materializeView(): HandlerResult
  @async def dematerializeView(): HandlerResult
  def source: ScenarioMap[_, _]
}

/**
 * [[ScenarioMap]] are updated using an updater that can be transformed into HandlerResult.
 *
 * [[ScenarioMapUpdater]] provides a builder-style API to construct various updates to a given ScenarioMap. It is
 * immutable and side-effect-free: all of the update methods return new ScenarioMapUpdater, and none of the updates are
 * applied until they are injected into the scenario through `toHandlerResult`.
 */
sealed trait ScenarioMapUpdater[K, V] {

  /**
   * Put multiple key-value pairs into the map. Existing values are overriden.
   */
  def putAll(m: Iterable[(K, V)]): ScenarioMapUpdater[K, V]

  /**
   * Unconditionally update a key.
   */
  def put(k: K, v: V): ScenarioMapUpdater[K, V]

  /**
   * Remove a key. This is a no-op if `k` is not in the map.
   */
  def remove(k: K): ScenarioMapUpdater[K, V]

  /**
   * Remove multiple keys. Equivalent to calling .remove multiple times.
   */
  def removeAll(ks: Iterable[K]): ScenarioMapUpdater[K, V]

  /**
   * Delete all the entries in the map.
   */
  def clear: ScenarioMapUpdater[K, V]

  /**
   * Replace all entries in this map by those in `m`.
   */
  def replace(m: Map[K, V]): ScenarioMapUpdater[K, V]

  /**
   * Create a handler result that encodes all the updates in this updater.
   */
  def toHandlerResult: HandlerResult
}

@entity private[util] sealed abstract class ScenarioMapImpl[K, V](tag: AnyRef, params: Parameters[K])
    extends ScenarioMapView[K, V] { outer: ScenarioMap[K, V] =>
  import ScenarioMapImpl._

  @node def keySet: Set[K]

  // View overrides
  final protected val emptyUpdater: ScenarioMapUpdater[K, V] = new ScenarioMapUpdaterImpl(Nil)

  final protected def indexFor[K1](target: Index[K, K1]): ScenarioMapView[K1, Set[V]] = {
    // Impressive inference by the scala compiler right here.
    params.indices.collectFirst { case `target` => target } match {
      case None =>
        throw new NoSuchElementException(s"index ${target} wasn't built into this map")
      case Some(index) =>
        // Ok to "recreate" because of Entity identity semantics
        IndexMap.from(this, index)
    }
  }

  private class ScenarioMapUpdaterImpl(log: List[UpdateLog[K, V]]) extends ScenarioMapUpdater[K, V] {
    private def copy(newLog: List[UpdateLog[K, V]]) = new ScenarioMapUpdaterImpl(newLog)

    def putAll(m: Iterable[(K, V)]): ScenarioMapUpdater[K, V] = copy(log match {
      case Put(map) :: rest => Put(map ++ m) :: rest
      case otherwise        => Put(m.toMap) :: otherwise
    })

    def put(k: K, v: V): ScenarioMapUpdater[K, V] = copy(log match {
      case Put(map) :: rest => Put(map + (k -> v)) :: rest
      case otherwise        => Put(Map(k -> v)) :: otherwise
    })

    def remove(k: K): ScenarioMapUpdater[K, V] = copy(log match {
      case Remove(prev) :: rest => Remove(prev + k) :: rest
      case otherwise            => Remove(Set(k)) :: otherwise
    })

    def removeAll(ks: Iterable[K]): ScenarioMapUpdater[K, V] = copy(log match {
      case Remove(prev) :: rest => Remove(prev ++ ks) :: rest
      case otherwise            => Remove(ks.toSet) :: otherwise
    })

    // Both replace and clear don't need earlier steps at all
    def clear: ScenarioMapUpdater[K, V] = copy(ScenarioMapImpl.clear[K, V] :: Nil)
    def replace(m: Map[K, V]): ScenarioMapUpdater[K, V] = copy(Replace(m) :: Nil)

    def toHandlerResult: HandlerResult = {
      // Order here is important!
      //
      // The materialization step will need to look at the newly inserted tweaks so it needs to happen at the end!

      // applies all the update steps to this map
      val updates = log.reverse
        .foldLeft(HandlerResult.empty) {
          // On clear, we drop previous updates and replace everything with a clear entity instance.
          case (_, _: Clear.type)      => Immediately(store.clear)
          case (prev, Put(updated))    => prev.andThen(Immediately(store.put(updated)))
          case (prev, Remove(dropped)) => prev.andThen(Immediately(store.drop(dropped)))
          case (prev, Replace(newMap)) => prev.andThen(Immediately(store.replace(newMap)))
        }
      params.indices.foldLeft(updates) { case (hr, index) => hr.andThen(IndexMap.from(outer, index).update) }
    }
  }

  // also used by indices!
  private[util] def createStore[K, V](whoFor: AnyRef): Store[K, V] = {
    params.storageType match {
      case Builder.Direct => Store.direct(whoFor)
    }
  }

  protected val store: Store[K, V] = createStore(this)
}

object Views {
  @entity class MapValues[K, V1, V](val parent: ScenarioMapView[K, V1], mapping: V1 => V @node)
      extends ScenarioMapView[K, V] {
    @node override def get(k: K): Option[V] = parent.get(k).map(mapping.apply)
    @node override def keySet: Set[K] = parent.keySet
  }
  MapValues.get.setCacheable(false)
  MapValues.keySet.setCacheable(false)

  @entity class FilterKeys[K, V](val parent: ScenarioMapView[K, V], filter: K => Boolean @node)
      extends ScenarioMapView[K, V] {
    @node override def get(k: K): Option[V] = if (filter(k)) parent.get(k) else None
    @node override def keySet: Set[K] = parent.keySet.aseq.filter(filter(_))
  }
  FilterKeys.get.setCacheable(false)
  FilterKeys.keySet.setCacheable(false)

  @entity class GroupBy[K1, K, V](val parent: ScenarioMapView[K1, V], groupBy: K1 => K @node)
      extends ScenarioMapView[K, Set[V]] {

    @node override def get(k: K): Option[Set[V]] = {
      parent.keySet.aseq.groupBy(groupBy(_)).get(k).map(_.aseq.flatMap(parent.get))
    }

    @node override def keySet: Set[K] = {
      val out = Set.newBuilder[K]
      for { k <- parent.keySet } out += groupBy(k)
      out.result()
    }
  }
  GroupBy.get.setCacheable(false)
  GroupBy.keySet.setCacheable(false)
}

object ScenarioMapImpl {
  private sealed trait UpdateLog[K, +V]
  private def clear[K, V]: UpdateLog[K, V] = Clear.asInstanceOf[UpdateLog[K, V]]

  private final case object Clear extends UpdateLog[Nothing, Nothing]
  private final case class Put[K, +V](map: Map[K, V]) extends UpdateLog[K, V]
  private final case class Remove[K, V](k: Set[K]) extends UpdateLog[K, V]
  private final case class Replace[K, V](map: Map[K, V]) extends UpdateLog[K, V]
}
