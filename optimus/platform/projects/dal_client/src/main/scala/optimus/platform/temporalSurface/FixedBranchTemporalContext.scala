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
package optimus.platform.temporalSurface

import optimus.graph.Settings
import optimus.platform.temporalSurface.impl.BranchTemporalSurfaceImpl
import optimus.platform.temporalSurface.impl.FixedTemporalContextImpl
import optimus.platform.temporalSurface.impl.TemporalContextTrace
import optimus.utils.MacroUtils.SourceLocation

private[optimus] object FixedBranchTemporalContext {
  def apply(scope: TemporalSurfaceScopeMatcher, children: List[FixedTemporalSurface], tag: Option[String])(implicit
      sourceLocation: SourceLocation): FixedBranchTemporalContext = {
    new FixedBranchTemporalContext(scope, children, tag, sourceLocation)
  }
}

final class FixedBranchTemporalContext(
    val scope: TemporalSurfaceScopeMatcher,
    val children: List[FixedTemporalSurface],
    override private[optimus] val tag: Option[String],
    override private[optimus] val sourceLocation: SourceLocation)
    extends BranchTemporalSurfaceImpl
    with FixedTemporalContextImpl {

  type childType = FixedTemporalSurface

  if (Settings.traceCreateTemporalContext) {
    TemporalContextTrace.traceCreated(this)
  }

  override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[FixedBranchTemporalContext]
}
