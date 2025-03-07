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
import optimus.platform.util.ScenarioMap
import optimus.platform.util.ScenarioMapView
import optimus.ui.HandlerStep
import optimus.ui.Immediately

/**
 * A scenario map view which is populated using data from a source map
 */
@entity abstract class IndexMap[K, K1, V](source: ScenarioMap[K, V]) extends ScenarioMapView[K1, Set[V]] {
  protected val store = source.createStore[K1, Set[K]](this)

  private[util] def update: HandlerStep

  @node override def get(k: K1): Option[Set[V]] = {
    store.get(k).map(_.apar.flatMap(source.get))
  }
  @node override def keySet: Set[K1] = store.keySet
}

@entity final class IndexMapWithSyncIndex[K, K1, V](source: ScenarioMap[K, V], index: K => K1)
    extends IndexMap[K, K1, V](source) {
  override private[util] def update: HandlerStep = Immediately {
    store.replace(source.keySet.groupBy(index))
  }
}

@entity final class IndexMapWithAsyncIndex[K, K1, V](source: ScenarioMap[K, V], index: K => K1 @node)
    extends IndexMap[K, K1, V](source) {
  @node @scenarioIndependent private def reindex = source.keySet.apar.groupBy(index(_))
  override private[util] def update: HandlerStep = Immediately {
    store.replace(reindex)
  }
}

object IndexMap {
  def from[K, K1, V](source: ScenarioMap[K, V], index: Index[K, K1]): IndexMap[K, K1, V] = {
    index match {
      case AsyncIndex(by) => IndexMapWithAsyncIndex(source, by)
      case SyncIndex(by)  => IndexMapWithSyncIndex(source, by)
    }
  }

  IndexMap.get.setCacheable(false)
  IndexMap.keySet.setCacheable(false)
  IndexMapWithAsyncIndex.reindex.setCacheable(false)
}
