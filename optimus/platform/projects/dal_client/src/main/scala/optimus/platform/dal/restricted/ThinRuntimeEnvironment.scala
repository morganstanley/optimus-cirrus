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
package optimus.platform.dal.restricted

import java.util.concurrent.ConcurrentHashMap
import optimus.breadcrumbs.ChainedID
import optimus.config.RuntimeComponents
import optimus.platform.dal.EntityResolver
import optimus.platform.dal.RestrictedEntityResolver
import optimus.platform.dal.RuntimeProperties
import optimus.platform.EvaluationContext
import optimus.platform.RuntimeEnvironment
import optimus.platform.ScenarioStack
import optimus.platform.UntweakedScenarioState
import optimus.platform.dal.session.RolesetMode.SpecificRoleset
/*
**
 * Only Dist should use this runtime environment
 *
 * @param runtimeConfig
 * @param resolver
 */

private[optimus] object ThinUntweakedScenarioStateCache {
  private val cache: ConcurrentHashMap[(SpecificRoleset, EntityResolver), UntweakedScenarioState] =
    new ConcurrentHashMap[(SpecificRoleset, EntityResolver), UntweakedScenarioState]()

  protected[restricted] def get(
      roles: SpecificRoleset,
      entityResolver: RestrictedEntityResolver): UntweakedScenarioState = {
    val key = (roles, entityResolver.unrestricted)
    cache.computeIfAbsent(
      key,
      _ => {
        val env = EvaluationContext.env
        val chainId = env.config.runtimeConfig.rootID
        val newConfig = env.config.runtimeConfig.withOverride(RuntimeProperties.DsiSessionRolesetModeProperty, roles)
        val runtimeComponent = new ThinRuntimeComponents(newConfig, entityResolver)
        UntweakedScenarioState(new ThinRuntimeEnvironment(runtimeComponent, entityResolver, chainId))
      }
    )
  }

  protected[optimus /*platform*/ ] def clear(): Unit = cache.clear()
}

private[optimus] object ThinUntweakedScenarioState {
  def apply(roles: SpecificRoleset, entityResolver: RestrictedEntityResolver): UntweakedScenarioState =
    ThinUntweakedScenarioStateCache.get(roles, entityResolver)
}

private[optimus] final class ThinRuntimeEnvironment(
    config: RuntimeComponents,
    entityResolver: RestrictedEntityResolver,
    chainId: ChainedID)
    extends RuntimeEnvironment(config, entityResolver) {
  override lazy val id: ChainedID = chainId
}
