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

import optimus.entity.IndexInfo
import optimus.platform.TemporalContext
import optimus.platform.dal.EntityEventModule.EntityOps
import optimus.platform.dal.QueryTemporality
import optimus.platform.internal.TemporalSource
import optimus.platform.node
import optimus.platform.bitemporal.EntityBitemporalSpace

trait KeyedEntityCompanionBase[T <: Entity] extends EntityCompanionBase[T] {
  val $keyInfo: IndexInfo[T, _] // gets overridden with a more specific type
  type KeyType = $keyInfo.PropType

  @node def bitemporalSpaceForKey(key: $keyInfo.PropType): EntityBitemporalSpace[T] = {
    EntityOps.getBitemporalSpace[T](
      $keyInfo.asInstanceOf[IndexInfo[T, KeyType]].makeKey(key).toSerializedKey,
      QueryTemporality.All,
      TemporalSource.loadContext.unsafeTxTime)
  }

  @node def bitemporalSpaceForKey(
      key: $keyInfo.PropType,
      fromContextOption: Option[TemporalContext],
      toContextOption: Option[TemporalContext]): EntityBitemporalSpace[T] = {
    val rtt = TemporalSource.loadContext.unsafeTxTime
    val (vtInterval, ttInterval) =
      EntityOps.getVtTtIntervalsFromTemporalContexts(fromContextOption, toContextOption, rtt)

    EntityOps
      .getBitemporalSpace[T](
        $keyInfo.asInstanceOf[IndexInfo[T, KeyType]].makeKey(key).toSerializedKey,
        QueryTemporality.BitempRange(vtInterval, ttInterval, inRange = false),
        rtt)
  }
}

//noinspection ScalaUnusedSymbol: used from entityplugin
abstract class KeyedEntityCompanionCls[T <: Entity] extends KeyedEntityCompanionBase[T]
