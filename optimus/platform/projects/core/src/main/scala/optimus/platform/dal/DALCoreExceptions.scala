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
import optimus.exceptions.RTExceptionTrait
import optimus.graph.NodeTaskInfo
import optimus.platform.TemporalContext
import optimus.platform.storable.EntityReference
import optimus.platform.dsi.bitemporal.MessageContainsDALEnv
import optimus.platform.storable.Entity

/**
 * Base class for all DAL exceptions
 */
abstract class DALException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)
    with MessageContainsDALEnv {
  def this(message: String) = this(message, null)
}
abstract class DALRTException(message: String, cause: Throwable)
    extends DALException(message, cause)
    with RTExceptionTrait {
  def this(message: String) = this(message, null)
}

/**
 * A link resolution failure.
 */
class LinkResolutionException(val ref: EntityReference, val temporalContext: TemporalContext, msg: String)
    extends DALRTException(msg) {
  def this(r: EntityReference, temporalContext: TemporalContext) =
    this(r, temporalContext, s"Invalid entity reference: $r@(temporalContext=$temporalContext)")
}

// NB: This ctor MUST NOT call sourceEntity.toString, as the Entity toString method will attempt to resolve any links in the key/index
// it decides to print, which may be the exact property that we are reporting the exception for!
class SourcedLinkResolutionException(
    r: EntityReference,
    temporalContext: TemporalContext,
    val sourceEntity: Entity,
    val propertyInfo: NodeTaskInfo)
    extends LinkResolutionException(
      r,
      temporalContext,
      s"Invalid entity reference from (${sourceEntity.getClass}@${sourceEntity.dal$entityRef}).${propertyInfo.name}: $r@(temporalContext=$temporalContext)"
    )
