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
package optimus.buildtool.builders

import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.config._
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.FileDiff
import optimus.graph.CancellationScope
import optimus.platform._

class TrackingBuilder(
    underlyingBuilder: NodeFunction0[StandardBuilder],
    workspace: TrackedWorkspace
) extends Builder {

  @async override def build(
      scopesToBuild: Set[ScopeId],
      postBuilder: PostBuilder,
      modifiedFiles: Option[FileDiff]
  ): BuildResult = {
    val cancellationScope = CancellationScope.newScope()
    val listener = ObtTrace.current
    workspace.rescan(cancellationScope, listener).thenCompose { _ =>
      workspace.run(cancellationScope, listener)(underlyingBuilder().build(scopesToBuild, postBuilder, modifiedFiles))
    }
  }.get()

}
