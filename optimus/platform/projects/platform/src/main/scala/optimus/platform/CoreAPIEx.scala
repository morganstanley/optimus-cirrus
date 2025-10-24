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
package optimus.platform

import optimus.graph.Node
import optimus.platform.annotations._
import optimus.platform.pickling.PickledInputStream
import optimus.platform.pickling.PickledMapWrapper
import optimus.platform.pickling.PickledProperties
import optimus.platform.pickling.Pickler
import optimus.platform.pickling.PropertyMapOutputStream

//noinspection ScalaUnusedSymbol  - used in versioning macros
object CoreAPIEx {
  @nodeSync
  @nodeSyncLift
  @expectingTweaks
  @scenarioIndependentInternal
  def givenTC[T](tc: TemporalContext)(@nodeLift @nodeLiftByName f: => T): T =
    EvaluationContext.givenL(tc2Scenario(tc), f)
  def givenTC$withNode[T](tc: TemporalContext)(f: Node[T]): T =
    EvaluationContext.given(tc2Scenario(tc), f).get
  def givenTC$queued[T](tc: TemporalContext)(f: Node[T]): Node[T] =
    EvaluationContext.given(tc2Scenario(tc), f).enqueueAttached

  private def tc2Scenario(tc: TemporalContext): Scenario = {
    Scenario(SimpleValueTweak(loadContext$newNode)(tc))
  }

  def pickle[T](pickler: Pickler[T], v: T): Any = {
    val out = new PropertyMapOutputStream()
    pickler.pickle(v, out)
    out.value
  }

  def pickledMapWrapper(properties: PickledProperties, temporalContext: TemporalContext): PickledInputStream = {
    new PickledMapWrapper(properties, temporalContext)
  }
}
