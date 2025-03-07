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
package optimus.platform.util.scenariomap

import optimus.platform._
import optimus.ui.ClearEntityInstanceTweaksGesture
import optimus.ui.ClearTweaksGesture
import optimus.ui.HandlerResult

// Entity that actually stores ScenarioMap data. Interface extracted here in case we want to set up different stores.
@entity sealed abstract class Store[K, V] {
  @node def keySet: Set[K]
  @node def get(k: K): Option[V]
  @node def transformIntoMap: Map[K, V]
  def clear: HandlerResult
  @async def put(updated: Map[K, V]): Seq[Tweak]
  @async def drop(removed: Set[K]): HandlerResult
  def replace(newData: Map[K, V]): HandlerResult
}

object Store {
  // we can implement alternate storage if needed
  def direct[K, V](tag: AnyRef): Store[K, V] = Direct.Impl(tag)
}

private[scenariomap] object Direct {
  @entity final class Impl[K, V](tag: AnyRef) extends Store[K, V] {
    @node(tweak = true) def keySet: Set[K] = Set.empty
    @node(tweak = true) def get(k: K): Option[V] = None

    @node def transformIntoMap: Map[K, V] = {
      val out = Map.newBuilder[K, V]
      for (el <- keySet) out += (el -> get(el).get)
      out.result()
    }

    def clear: HandlerResult = HandlerResult.withGesture(ClearEntityInstanceTweaksGesture(this))

    @async def put(updated: Map[K, V]): Seq[Tweak] = {
      val values = updated.toSeq.map { case (k, v) => Tweak.byValue(get(k) := Some(v)) }
      val oldKeys = keySet
      val newKeys = keySet ++ updated.keySet
      if (oldKeys == newKeys) values
      else values ++ Tweaks.byValue(keySet := newKeys)
    }

    @async def drop(removed: Set[K]): HandlerResult = {
      val oldKeys = keySet
      val newKeys = keySet -- removed
      if (oldKeys == newKeys) HandlerResult.empty
      else
        HandlerResult
          .withGesture(ClearTweaksGesture(removed.map(k => nodeKeyOf(get(k)))))
          .withTweaks(Tweaks.byValue(keySet := newKeys))
    }

    def replace(newData: Map[K, V]): HandlerResult = {
      val oldKeys = keySet
      val newKeys = newData.keySet

      locally {
        // Delete any keys that are in the old map but not in the new map
        val toDrop = oldKeys -- newKeys
        if (toDrop.isEmpty) HandlerResult.empty
        else
          HandlerResult
            .withGesture(ClearTweaksGesture(toDrop.map(k => nodeKeyOf(get(k)))))
      }.withTweaks({
        // then update all the values to the one in the current map
        val values = newData.toSeq.map { case (k, v) => Tweak.byValue(get(k) := Some(v)) }
        if (oldKeys == newKeys) values
        else values ++ Tweaks.byValue(keySet := newKeys)
      })
    }
  }

  Impl.get.setCacheable(false)
  Impl.keySet.setCacheable(false)
  Impl.transformIntoMap.setCacheable(false)
}
