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
import optimus.platform.LocationTag
import optimus.platform.annotations.withLocationTag
import optimus.platform.node
import optimus.platform.util.ScenarioMap
import optimus.platform.util.scenariomap.Builder.StorageType

sealed trait Index[-K, +K1]
final case class AsyncIndex[-K, +K1](by: K => K1 @node) extends Index[K, K1]
final case class SyncIndex[-K, +K1](by: K => K1) extends Index[K, K1]

trait Builder[K, V] {

  /**
   * Use default "direct" storage, with one tweak per K,V pair.
   * @return
   */
  def direct: Builder[K, V]

  /**
   * Add an auxiliary index.
   */
  def withIndex(index: Index[K, _]): Builder[K, V]

  @withLocationTag
  def build: ScenarioMap[K, V]
  def build(lt: LocationTag): ScenarioMap[K, V]
}

final case class Parameters[K](
    storageType: StorageType,
    indices: Seq[Index[K, _]]
)

object Builder {
  private[util] sealed trait StorageType
  private[util] final case object Direct extends StorageType

  private val defaults = BuilderImpl[Nothing, Nothing](Direct, Set.empty[Index[Nothing, _]])

  def empty[K, V] = defaults.asInstanceOf[Builder[K, V]]
}

private[scenariomap] final case class BuilderImpl[K, V](storage: StorageType, indices: Set[Index[K, _]])
    extends Builder[K, V]() {
  import Builder._

  private def params = Parameters(storage, indices.toSeq)

  def direct = copy(storage = Direct)

  def withIndex(index: Index[K, _]): Builder[K, V] = copy(indices = indices + index)
  def build: ScenarioMap[K, V] = ScenarioMap.uniqueInstance[K, V](null, params)
  def build(lt: LocationTag): ScenarioMap[K, V] = ScenarioMap[K, V](lt, params)
}
