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
package optimus.platform.dal.versioning.worker

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
// import msjava.pool.ManagedPool
// import msjava.pool.base.BasePoolableObjectFactory
import msjava.slf4jutils.scalalog.getLogger
import optimus.config.OptimusConfigurationException
import optimus.config.RuntimeConfiguration
import optimus.dsi.base.DSIUntweakedScenarioState
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.session.ClientSessionInfo
import optimus.platform._
import optimus.platform.dal.ClientSideDSI
import optimus.platform.dal.RuntimeProperties
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalLocation
import optimus.platform.dal.config.DalUri
import optimus.platform.runtime.RuntimeComponents

private[optimus] object VersioningWorkerPool {
  private val log = getLogger(this)

  private val poolSize: Int = Integer.getInteger("optimus.dsi.versioning.pool.size", 8)
  private val cacheTime: Int = Integer.getInteger("optimus.dsi.versioning.userSessionCacheExpirySecs", 600)
  private val cacheSize: Int = Integer.getInteger("optimus.dsi.versioning.userSessionCacheSize", 3000)

  private val ThreadName = "VersioningWorkerPool"
}

private[optimus] class VersioningWorkerPool(runtimeConfig: RuntimeConfiguration) {
  self =>
  import VersioningWorkerPool._

  private val serial = new AtomicInteger(0)
  private val uriOpt = runtimeConfig.getString(RuntimeProperties.DsiUriProperty)
  private val appIdOpt = runtimeConfig.getString(RuntimeProperties.DsiAppIdProperty).map(DalAppId(_))
  require(uriOpt.isDefined && appIdOpt.isDefined)
  private val service = OptimusService.createBgExecutor(
    new OptimusTask {
      override val appId: DalAppId = appIdOpt.get
      override val dalLocation: DalLocation = DalUri(uriOpt.get)
    },
    ThreadName
  )

  /* private val pool =
    new ManagedPool[VersioningPoolWorker](new BasePoolableObjectFactory[VersioningPoolWorker] {
      override def createObject = {
        val worker =
          new VersioningPoolWorker(s"vers-work-${serial.getAndIncrement}", service, untweakedScenarioStateCache)
        log.info(s"created VersioningPoolWorker: ${worker.name}")
        worker
      }

      override def destroyObject(worker: VersioningPoolWorker) = {
        worker.close()
        log.info(s"closed VersioningPoolWorker: ${worker.name}")
      }

      override def validateObject(obj: VersioningPoolWorker) = true
    })

  pool.setMaxActive(poolSize)
  pool.afterPropertiesSet */

  def close(): Unit = {
    // pool.destroy()
  }

  /**
   * Executes the passed block of code using the primary dsi and not with user specific dsi.
   */
  def exec[T](f: => T): T = ??? /* {
    val svc = pool.borrowObject()
    try {
      val ret = svc.exec(f)
      ret
    } finally {
      pool.returnObject(svc)
    }
  } */

  /**
   * Executes the passed block of code using the user specific dsi.
   */
  def exec[T](session: ClientSessionInfo)(f: => T): T = ??? /* {
    val svc = pool.borrowObject()
    try {
      val ret = svc.exec(session)(f)
      ret
    } finally {
      pool.returnObject(svc)
    }
  } */

  private val untweakedScenarioStateCache = CacheBuilder
    .newBuilder()
    .maximumSize(cacheSize)
    .expireAfterWrite(cacheTime, TimeUnit.SECONDS)
    .build(new CacheLoader[ClientSessionInfo, UntweakedScenarioState] {
      override def load(session: ClientSessionInfo): UntweakedScenarioState = {
        // TODO (OPTIMUS-14123): at the moment we can only support default context. To
        // support others we will need to create DSIs with the correct context here, and also fix on-behalf sessions
        // to be able to support non-default context (at the moment it relies on metadata which is only written in the
        // default context)
        val config = runtimeConfig
          .withOverride(RuntimeProperties.DsiSessionRealIdProperty, session.realId)
          .withOverride(RuntimeProperties.DsiSessionRolesetModeProperty, session.rolesetMode)
          .withOverride(RuntimeProperties.DsiZoneProperty, session.applicationIdentifier.zoneId.underlying)
          .withOverride(RuntimeProperties.DsiAppIdProperty, session.applicationIdentifier.appId.underlying)
          .withOverride(RuntimeProperties.DsiOnBehalfSessionToken, session.onBehalfSessionToken)
          .withOverride(RuntimeProperties.DsiSessionEstablishmentTimeProperty, session.establishmentTime)

        val runtime = new RuntimeComponents(config) {
          override def createDsi() = {
            val dsi = super.createDsi()
            val partitionMap = config
              .get(PartitionMap.PartitionMapProperty)
              .getOrElse(
                throw new OptimusConfigurationException("Couldn't find the PartitionMap in the RuntimeConfiguration.")
              )
              .asInstanceOf[PartitionMap]
            dsi match {
              case d: ClientSideDSI => ReadOnlyClientSideDsi(d, partitionMap)
              case o                => ReadOnlyDsi(o, partitionMap)
            }
          }
        }
        DSIUntweakedScenarioState(runtime, runtime.createEntityResolver())
      }
    })
}

private[optimus] class VersioningPoolWorker(
    val name: String,
    val service: OptimusBgExecutor,
    untweakedScenarioStateCache: LoadingCache[ClientSessionInfo, UntweakedScenarioState]
) {

  /**
   * Executes the passed block of code using the primary dsi and not with user specific dsi.
   */
  def exec[TResult](f: => TResult): TResult = service.runSync(f)

  /**
   * Executes the passed block of code using the user specific dsi.
   */
  def exec[TResult](session: ClientSessionInfo)(f: => TResult): TResult = service.runSync {
    require(EvaluationContext.isInitialised, "Cannot execute tasks without an established evaluation context")
    val scenarioState = EvaluationContext.untweakedScenarioState
    try {
      EvaluationContext.initializeWithNewInitialTime(untweakedScenarioStateCache.get(session))
      f
    } finally {
      EvaluationContext.initializeWithNewInitialTime(scenarioState)
    }
  }

  def close(): Unit = service.close()
}
