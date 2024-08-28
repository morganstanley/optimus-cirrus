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
package optimus.graph.diagnostics.sampling
import optimus.graph.OGLocalTables
import optimus.graph.OGSchedulerContext
import optimus.graph.TwkResolver
import optimus.graph.cache.NodeCache
import optimus.graph.diagnostics.ap.FrameMatcherProvider
import optimus.graph.diagnostics.ap.SampledTimersExtractor.FrameMatcher
import optimus.graph.diagnostics.ap.SampledTimersExtractor.matcher

object GraphFrameMatcherProvider {
  val graph = "graph"
  val syncStack = "syncStack"
  val cache = "cache"
  val tweakLUS = "tweakLUS"
  val tweakLUA = "tweakLUA"
  val localTables = "localTables"
}

//noinspection ScalaUnusedSymbol // ServiceLoader
class GraphFrameMatcherProvider extends FrameMatcherProvider {
  override def frameMatchers: Seq[FrameMatcher] = {
    import optimus.utils.StringTyping._
    import GraphFrameMatcherProvider._

    val ogscRun = isEqualTo(methodFrameString[OGSchedulerContext]("run"))
    val ogscRunAndWait = isEqualTo(methodFrameString[OGSchedulerContext]("runAndWait"))
    Seq(
      matcher(graph, ogscRun or ogscRunAndWait),
      matcher(syncStack, ogscRun or ogscRunAndWait, ogscRunAndWait),
      matcher(cache, isEqualTo(objectMethodFrameString(NodeCache, "cacheLookup"))),
      matcher(tweakLUS, isEqualTo(methodFrameString[TwkResolver[_]]("syncResolve"))),
      matcher(tweakLUA, isEqualTo(methodFrameString[TwkResolver[_]]("asyncResolve"))),
      matcher(localTables, startsWith(methodFrameString[OGLocalTables]("forAllRemovables")))
    )
  }
}
