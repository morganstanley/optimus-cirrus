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

import optimus.platform._
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.annotations.expectingTweaks
import optimus.platform.util.ElevatedUtils

trait DALLegacy {
  object legacy {

    /**
     * Creates a persist block, inside which entities can be manipulated and the persisted.
     *
     * The persist block works in concert with the DAL.persist and DAL.merge functions, which are used to associate and
     * ultimately persist entities that have been created or loaded outside of the persist block.
     *
     * Any entities that are loaded inside of the persist block automatically become merged into it.
     */
    @closuresEnterGraph def persist[T](f: => T): T = {
      ElevatedUtils.throwOnElevated()
      // identity(...) is required for Loom: without it, Scala optimizes this by calling f rather than creating () => f
      DALBlock.execute(resolver.createScenario(Nil), resolver.createPersistBlock, asAsync(() => identity(f)))._1
    }

    /**
     * Creates a persist block with some tweaks. The primary purpose of persistGiven is to persist entities at a
     * specific valid time. This would be done like so:
     *
     * persistGiven(validTime := UTCDateTime(2012, 1, 2, 3, 4, 5)) { ... }
     *
     * See the docs for persist for more information about persist blocks in general.
     */
    @closuresEnterGraph @expectingTweaks
    def persistGiven[T](tweaks: Tweak*)(f: => T): T = {
      ElevatedUtils.throwOnElevated()
      // identity(...) is required for Loom: without it, Scala optimizes this by calling f rather than creating () => f
      DALBlock.execute(resolver.createScenario(tweaks), resolver.createPersistBlock, asAsync(() => identity(f)))._1
    }

    @closuresEnterGraph
    def legacyEvent[T](e: BusinessEvent)(f: => T): T = {
      ElevatedUtils.throwOnElevated()
      resolver.event(e)(f)
    }

    @closuresEnterGraph
    def legacyTransaction[T](f: => T): T = {
      ElevatedUtils.throwOnElevated()
      resolver.inNewAppEvent(f)._1
    }

    private[this] def resolver: ResolverImpl = EvaluationContext.env.entityResolver.asInstanceOf[ResolverImpl]

  }
}
