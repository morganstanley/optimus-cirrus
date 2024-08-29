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
package optimus.platform.dal

import java.time.Instant

import optimus.platform.bitemporal.EntityBitemporalSpace
import optimus.platform._
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.storable.Entity

/**
 * Used by "entity.bitemporalSpaceWithFilter(...)" to inject the implementation from priql_dal. priql_dal depends on
 * dal_client, thus we need this IoC (inverse of control).
 */
trait BitemporalSpaceResolver {
  @scenarioIndependent @node def getEntityBitemporalSpace[T <: Entity](
      e: T,
      predicate: LambdaElement,
      fromTemporalContext: Option[TemporalContext],
      toTemporalContext: Option[TemporalContext]): EntityBitemporalSpace[T]
}
