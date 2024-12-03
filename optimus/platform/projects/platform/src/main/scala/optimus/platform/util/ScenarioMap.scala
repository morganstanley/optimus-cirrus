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
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs
import optimus.ui.ClearEntityInstanceTweaksGesture
import optimus.ui.ClearTweaksGesture
import optimus.ui.HandlerResult
import optimus.ui.Immediately

/**
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
 * ScenarioMap is meant as a drop-in replacement for TweakableMap / TweakableBackedMap
 */
@entity class ScenarioMap[K, V](tag: LocationTag) extends ScenarioMapImpl[K, V](tag) {

  /**
   * Get the value associated with a specific key.
   *
   * This call will be invalidated only if the key's value changes.
   */
  @node def get(k: K): Option[V] = getImpl(k)

  /**
   * Create a lazy view of this map. See [[ScenarioMapView]] for details.
   */
  def view: ScenarioMapView[K, V] = asView

  /**
   * Create an updater for this map. See [[ScenarioMapUpdater]] for details. Note that updates will only be applied
   * as part of the `HandleResult` returned by the updater
   * @return
   */
  def updater: ScenarioMapUpdater[K, V] = createUpdater

  /**
   * Transforms the [[ScenarioMap]] into a standard immutable Scala map. This is an expensive method that will create
   * invalidations on every update of the [[ScenarioMap]] and should be avoided whenever possible.
   */
  @node def materialize: Map[K, V] = {
    keysImpl.apar
      .map(k => k -> getImpl(k))
      .collect { case (k, Some(m)) => k -> m }
      .toMap
  }

  /**
   * Keys that this map has in the current scenario, as a standard Scala immutable Set.
   *
   * This call will be invalidated whenever the key set changes (that is, when keys are added or removed), but not when
   * values are updated.
   */
  @node def keySet: Set[K] = keysImpl
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
@entity abstract class ScenarioMapView[K, V] extends MapOrView[K, V] {
  @node def get(k: K): Option[V]

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
   * The view `.get()` method will be invalidated whenever the key set on the map changes (that is, when elements are
   * added or removed, but not when elements are updated.)
   */
  @alwaysAutoAsyncArgs
  def groupBy[K1](f: K => K1): ScenarioMapView[K1, Seq[V]] = ???
  def groupBy[K1](f: K => K1 @node): ScenarioMapView[K1, Seq[V]] = Views.GroupBy(this, f)
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
  @async def toHandlerResult: HandlerResult
}

// Implementation
@entity private[util] sealed trait MapOrView[K, V] {
  @node def get(k: K): Option[V]
  @node def keySet: Set[K]
}

@entity private[util] sealed abstract class ScenarioMapImpl[K, V](tag: AnyRef) extends MapOrView[K, V] { outer =>
  import ScenarioMapImpl._

  @node def keySet: Set[K]
  @node(tweak = true) protected def getImpl(k: K): Option[V] = None
  @node(tweak = true) protected def keysImpl: Set[K] = Set.empty
  protected def createUpdater: ScenarioMapUpdater[K, V] = new ScenarioMapUpdaterImpl(Nil)
  protected val asView = Views.Identity(this)

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

    def clear: ScenarioMapUpdater[K, V] = copy(ScenarioMapImpl.clear[K, V] :: Nil)

    def replace(m: Map[K, V]): ScenarioMapUpdater[K, V] = clear.putAll(m)

    @async def toHandlerResult: HandlerResult = {
      log.reverse.foldLeft(HandlerResult.empty) {
        // on clear, we drop previous updates and replace everything with a clear entity instance
        case (_, _: Clear.type) => HandlerResult.withGesture(ClearEntityInstanceTweaksGesture(outer))
        case (prev, Put(updated)) =>
          prev.andThen(
            Immediately(
              newKeys(updated.keySet) ++
                updated.toSeq.map { case (k, v) => Tweak.byValue(outer.getImpl(k) := Some(v)) })
          )
        case (prev, Remove(dropped)) =>
          prev.andThen(
            Immediately(
              HandlerResult.withGesture(
                ClearTweaksGesture(dropped.map(k => nodeKeyOf(outer.getImpl(k))))
              )
            )
          )
      }
    }
  }

  @async private def newKeys(added: Set[K]): Seq[Tweak] = {
    val before = keysImpl
    val after = before ++ added
    if (after == before) Nil
    else Tweaks.byValue(keysImpl := after)
  }
  @async private def removedKeys(removed: Set[K]): Seq[Tweak] = {
    val before = keysImpl
    val after = before -- removed
    if (after == before) Nil
    else Tweaks.byValue(keysImpl := after)
  }
}

object Views {
  @entity class Identity[K, V](orig: MapOrView[K, V]) extends ScenarioMapView[K, V] {
    @node override def get(k: K): Option[V] = orig.get(k)
    @node override def keySet: Set[K] = orig.keySet
  }

  @entity class MapValues[K, V1, V](orig: MapOrView[K, V1], f: V1 => V @node) extends ScenarioMapView[K, V] {
    @node override def get(k: K): Option[V] = orig.get(k).map(f.apply)
    @node override def keySet: Set[K] = orig.keySet
  }

  @entity class FilterKeys[K, V](orig: MapOrView[K, V], filter: K => Boolean @node) extends ScenarioMapView[K, V] {
    @node override def get(k: K): Option[V] = if (filter(k)) orig.get(k) else None
    @node override def keySet: Set[K] = orig.keySet.apar.filter(filter.apply)
  }

  @entity class GroupBy[K1, K, V](orig: MapOrView[K1, V], groupBy: K1 => K @node) extends ScenarioMapView[K, Seq[V]] {
    @async private def keyMap = orig.keySet.apar.groupBy(groupBy.apply)

    @node override def get(k: K): Option[Seq[V]] =
      keyMap.get(k).map { k => k.toSeq.apar.flatMap(orig.get) }

    @node override def keySet: Set[K] = keyMap.keySet
  }
}

object ScenarioMapImpl {
  private sealed trait UpdateLog[K, +V]
  private def clear[K, V]: UpdateLog[K, V] = Clear.asInstanceOf[UpdateLog[K, V]]

  private final case object Clear extends UpdateLog[Nothing, Nothing]
  private final case class Put[K, +V](map: Map[K, V]) extends UpdateLog[K, V]
  private final case class Remove[K, V](k: Set[K]) extends UpdateLog[K, V]
}
