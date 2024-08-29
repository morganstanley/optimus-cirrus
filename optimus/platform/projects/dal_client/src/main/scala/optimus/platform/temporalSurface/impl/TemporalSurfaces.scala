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
package optimus.platform.temporalSurface.impl

import optimus.platform.temporalSurface.FixedTemporalSurface
import optimus.platform.temporalSurface.TemporalSurfaceScopeMatcher
import optimus.graph.Settings
import optimus.platform._
import optimus.platform.annotations.deprecating
import optimus.platform.temporalSurface.DataFreeTemporalSurfaceMatchers
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingData
import optimus.utils.MacroUtils.SourceLocation

class FixedBranchTemporalSurfaceImpl(
    val scope: TemporalSurfaceScopeMatcher,
    val children: List[FixedTemporalSurface],
    override val tag: Option[String],
    override val sourceLocation: SourceLocation)
    extends BranchTemporalSurfaceImpl
    with FixedTemporalSurface {

  type childType = FixedTemporalSurface

  override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[FixedBranchTemporalSurfaceImpl]
}

@deprecating(
  "It is only used for RunCalc to trace DefaultTS, and will be deco after we remove DefaultTS"
)
final class FixedTracingTemporalSurface(
    val content: FixedTemporalSurface,
    tag: Option[String],
    sourceLocation: SourceLocation)
    extends FixedBranchTemporalSurfaceImpl(
      DataFreeTemporalSurfaceMatchers.allScope,
      content :: Nil,
      tag,
      sourceLocation) {

  @node override private[temporalSurface] def queryTemporalSurface(
      callingScope: List[TemporalSurface],
      operation: EntityQueryData,
      temporalContextChain: List[TemporalContext],
      considerDelegation: Boolean,
      tsProf: Option[TemporalSurfaceProfilingData]): EntityQueryData = {
    if (Settings.enableTraceSurface)
      TemporalContextTrace.traceQuery(callingScope, operation.operation, temporalContextChain, tag.getOrElse(""))
    super.queryTemporalSurface(callingScope, operation, temporalContextChain, considerDelegation, tsProf)
  }
}
