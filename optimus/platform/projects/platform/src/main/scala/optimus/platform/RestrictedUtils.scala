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

import optimus.graph._
import optimus.platform.AdvancedUtils.givenFullySpecifiedScenarioWithInitialTime$newNode
import optimus.platform.PluginHelpers.toNode
import optimus.platform.annotations._
import optimus.platform.internal.TemporalSource
import optimus.platform.runtime.InitialRuntime

import java.time.Instant

/** Very unusual API for rather special case. Even more so than AdvancedUtils */
object RestrictedUtils {
  private val initialRuntimeNodeKeys = Seq(nodeKeyOf(InitialRuntime.initialTime), nodeKeyOf(TemporalSource.initialTime))

  private val loadContextStoreContextNodeKeys =
    Seq(nodeKeyOf(TemporalSource.loadContext), nodeKeyOf(TemporalSource.validTimeStore))

  /** Removed time related tweaks (initialTimes) */
  @nodeSync
  @nodeSyncLift
  def removeScenariosWithInitialTime[T](initialTime: Instant, removeKeyList: Seq[NodeKey[_]])(
      @nodeLift @nodeLiftByName f: => T): T =
    removeScenariosWithInitialTime$withNode(initialTime, removeKeyList)(toNode(f _))
  // noinspection ScalaWeakerAccess (compiler plugin forwards to this def)
  final def removeScenariosWithInitialTime$withNode[T](initialTime: Instant, removeKeyList: Seq[NodeKey[_]])(
      f: Node[T]): T =
    removeScenariosWithInitialTime$newNode(initialTime, removeKeyList)(f).get
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def removeScenariosWithInitialTime$queued[T](initialTime: Instant, removeKeyList: Seq[NodeKey[_]])(
      f: Node[T]): NodeFuture[T] =
    removeScenariosWithInitialTime$newNode(initialTime, removeKeyList)(f).enqueueAttached

  final private[this] def removeScenariosWithInitialTime$newNode[T](
      initialTime: Instant,
      removeKeyList: Seq[NodeKey[_]])(f: Node[T]): Node[T] = {
    val scenarios = ScenarioStackMoniker.buildSSCacheIDArray(EvaluationContext.scenarioStack)
    val allRemoveKeys = initialRuntimeNodeKeys ++ removeKeyList
    val allRemovePropertyInfoNames = allRemoveKeys.map(_.propertyInfo.fullName())
    val cleanScenarios =
      scenarios
        .map(_.filterConserve({ twk: Tweak =>
          if (twk.target.hashKey != null) !allRemoveKeys.contains(twk.target.hashKey)
          else !allRemovePropertyInfoNames.contains(twk.target.propertyInfo.fullName())
        }))
        .reverse

    givenFullySpecifiedScenarioWithInitialTime$newNode(
      Scenario.toNestedScenarios(cleanScenarios),
      initialTime,
      inheritSIParams = true
    )(f)
  }

  /** Removed time related tweaks (initialTimes and loadContext) */
  @nodeSync
  @nodeSyncLift
  def removeLoadContextStoreContextAndScenariosWithInitialTime[T](initialTime: Instant, removeKeyList: Seq[NodeKey[_]])(
      @nodeLift @nodeLiftByName f: => T): T =
    removeLoadContextStoreContextAndScenariosWithInitialTime$withNode(initialTime, removeKeyList)(toNode(f _))
  // noinspection ScalaWeakerAccess (compiler plugin forwards to this def)
  final def removeLoadContextStoreContextAndScenariosWithInitialTime$withNode[T](
      initialTime: Instant,
      removeKeyList: Seq[NodeKey[_]])(f: Node[T]): T =
    removeLoadContextStoreContextAndScenariosWithInitialTime$newNode(initialTime, removeKeyList)(f).get
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  final def removeLoadContextStoreContextAndScenariosWithInitialTime$queued[T](
      initialTime: Instant,
      removeKeyList: Seq[NodeKey[_]])(f: Node[T]): NodeFuture[T] =
    removeLoadContextStoreContextAndScenariosWithInitialTime$newNode(initialTime, removeKeyList)(f).enqueueAttached

  final private[this] def removeLoadContextStoreContextAndScenariosWithInitialTime$newNode[T](
      initialTime: Instant,
      removeKeyList: Seq[NodeKey[_]])(f: Node[T]): Node[T] =
    removeScenariosWithInitialTime$newNode(initialTime, loadContextStoreContextNodeKeys ++ removeKeyList)(f)
}
