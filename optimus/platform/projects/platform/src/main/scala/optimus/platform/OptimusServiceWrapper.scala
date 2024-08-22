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

import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation

import scala.collection.mutable

class OptimusServiceWrapper private (val dalAppId: DalAppId, val env: DalEnv) {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  private val optimusTask = new OptimusTask {
    protected def appId: DalAppId = dalAppId
    override protected def dalLocation: DalLocation = env
  }

  private val optimusService = OptimusService.createBgExecutor(optimusTask)

  def runSync[R](f: => R): R = {
    log.info(s"Running $f synchronously in optimus environment")
    optimusService runSync f
  }

  def runSyncWithContextNow[R](f: TemporalContext => R): R = {
    log.info(s"Running $f synchronously in optimus environment with context vt,tt = At.now")
    optimusService runSync {
      val now = At.now
      val temporalContext = TemporalContext(now, now)
      f(temporalContext)
    }
  }
}

object OptimusServiceWrapper {
  private val bridgeInstances: mutable.Map[(DalEnv, DalAppId), OptimusServiceWrapper] = mutable.Map.empty
  def apply(env: DalEnv, app: DalAppId): OptimusServiceWrapper = synchronized {
    bridgeInstances.getOrElseUpdate((env, app), new OptimusServiceWrapper(app, env))
  }
}
