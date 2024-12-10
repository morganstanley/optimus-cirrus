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
package optimus.platform.runtime

import java.net.URI
import java.time.Instant
import msjava.slf4jutils.scalalog._
import msjava.zkapi.internal.ZkaContext
import optimus.breadcrumbs.Breadcrumbs
import optimus.config.OptimusConfigurationException
import optimus.config.RuntimeConfiguration
import optimus.config.{RuntimeComponents => CoreRuntimeComponents}
import optimus.dsi.partitioning.DefaultPartition
import optimus.graph.AlreadyCompletedNode
import optimus.graph.DiagnosticSettings
import optimus.graph.InstancePropertyTarget
import optimus.graph.PropertyNode
import optimus.graph.SITweakValueProvider
import optimus.graph.ScenarioStackShared
import optimus.graph.Scheduler
import optimus.graph.Settings
import optimus.graph.TweakNode
import optimus.graph.TweakValueProviderNode
import optimus.graph.diagnostics.InfoDumper
import optimus.graph.diagnostics.sampling.SamplingProfilerSwitch
import optimus.graph.{Settings => gs}
import optimus.platform.RuntimeEnvironment.KnownNames._
import optimus.platform.Tweak
import optimus.platform._
import optimus.platform.dal.DALEntityResolver
import optimus.platform.dal.DSIFactory
import optimus.platform.dal.EntityResolver
import optimus.platform.dal.RuntimeProperties
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dsi.bitemporal.DSI
import optimus.platform.dsi.bitemporal.NoDSI
import optimus.platform.internal.TemporalSource
import optimus.platform.internal.UninitializedInitialTimeException

import scala.annotation.tailrec

object RuntimeComponents {
  private val log = getLogger[RuntimeComponents]
  log.info("newCache is used")

  val DsiUriProperty = RuntimeProperties.DsiUriProperty

  def apply(c: RuntimeConfiguration) = new RuntimeComponents(c)
  def apply(c: RuntimeConfiguration, asyncConfigOpt: Option[DalAsyncConfig]) = new RuntimeComponents(c, asyncConfigOpt)

  def createEntityResolver(dsi: DSI): EntityResolver =
    DALEntityResolver(dsi)

  /** Extracts [[InitialRuntime.initialTime]] from a scenario stack */
  private[optimus] def getInitialRuntimeInitialTime(ss: ScenarioStack): Instant = ??? /* {
    ss.getNode(InitialRuntime.initialTime.key).get
  } */

  class WithConstantTime(rc: RuntimeConfiguration, t: Instant) extends RuntimeComponents(rc) {
    override protected[this] final def timeLookup(resolver: EntityResolver): Instant = t
  }
  // This is just used in tests at the moment..
  class WithMaxTime(rc: RuntimeConfiguration) extends RuntimeComponents(rc) {
    override protected[this] final def timeLookup(resolver: EntityResolver): Instant = {
      if (!EvaluationContext.isInitialised) EvaluationContext.initializeWithoutRuntime()
      val tts = resolver.serverTime.values
      require(tts.nonEmpty, "tt of at least one partition is expected to be defined. Got empty map")
      tts.max
    }
  }
}

@entity private object InitialRuntime {
  @node(tweak = true) private[optimus] def initialTime: Instant = TemporalSource.uninitializedInitialTime()
  initialTime_info.internalSetTweakMaskForDAL()
}

class RuntimeComponents(
    runtimeConfig: RuntimeConfiguration,
    val asyncConfigOpt: Option[DalAsyncConfig] = None,
    override private[optimus] val contextClassloader: ClassLoader = classOf[RuntimeComponents].getClassLoader
) extends CoreRuntimeComponents(runtimeConfig) {

  import gs.allowIllegalOverrideOfInitialRuntimeEnvironment

  protected[this] def timeLookup(resolver: EntityResolver): Instant = {
    @tailrec
    def getRuntimeComponent(rc: RuntimeConfiguration): Instant = {
      rc match {
        case i: ConfigOverrider =>
          getRuntimeComponent(i.config)

        case _: MockRuntimeConfiguration =>
          val serverTimes = resolver.serverTime
          require(serverTimes.values.nonEmpty, "At least one value is expected in serverTimes got empty")
          serverTimes.getOrElse(DefaultPartition, serverTimes.values.max)

        case _ =>
          val serverTime = resolver.serverTime
          serverTime.getOrElse(
            DefaultPartition, {
              RuntimeComponents.log.error(
                s"Unable to find DefaultPartition in the server time $serverTime. Using maximum available value instead.")
              require(
                serverTime.values.nonEmpty,
                "At least one value is expected to be available in serverTime. Got empty map")
              serverTime.values.max
            }
          )
      }
    }

    if (!EvaluationContext.isInitialised) EvaluationContext.initializeWithoutRuntime()
    getRuntimeComponent(runtimeConfig)
  }

  def createDsi(): DSI = {
    if (runtimeConfig.env != EnvNone) {
      val uriString = runtimeConfig
        .getString(RuntimeComponents.DsiUriProperty)
        .getOrElse(
          throw new OptimusConfigurationException("DSI URI is not defined: " + RuntimeComponents.DsiUriProperty))

      val uri = makeURI(uriString)

      val dsiFactory = createDsiFactory(uri)

      dsiFactory.createDSI(uri)
    } else NoDSI
  }

  protected def createDsiFactory(uri: URI) = new DSIFactory(runtimeConfig, asyncConfigOpt)

  protected def makeURI(uri: String): URI = {
    new URI(uri match {
      case "memory://" => "memory://default"
      case _           => uri
    })
  }

  def createEntityResolver(): EntityResolver = RuntimeComponents.createEntityResolver(createDsi())

  override def createRuntime(): RuntimeEnvironment = {
    val resolver = createEntityResolver()

    if (
      (runtimeConfig.env != EnvMock || DiagnosticSettings.getBoolProperty("breadcrumb.allowMockEnv", false))
      && runtimeConfig.env != EnvTest
      // TODO (OPTIMUS-21803): enable breadcrumbs for the AWS environment as well
      && runtimeConfig.env != EnvAws
      && runtimeConfig.env != EnvNone
    )
      maybeInitBreadCrumbs()
    createWithResolver(resolver)
  }

  private def getRootContext: ZkaContext = {
    val uri = makeURI(runtimeConfig.getString(RuntimeProperties.DsiUriProperty).get)
    ZkUtils.getRootContextForUriAuthority(uri.getAuthority)
  }

  private def maybeInitBreadCrumbs(): Unit = {
    // Nb. this is lazy.  Won't run unless appropriate publisher enabled.
    val rootContext = getRootContext
    Breadcrumbs.customizedInit(runtimeConfig.propertyMap, rootContext)
    SamplingProfilerSwitch.configure(runtimeConfig, rootContext)
  }

  private[optimus] def createWithResolver(resolver: EntityResolver): RuntimeEnvironment = {
    val env = new RuntimeEnvironment(this, resolver)
    env
  }

  override private[optimus] def newTopScenario(resolver: EntityResolver): Scenario = {
    import Settings.initialTimeResolutionMode
    val scenario = {
      val runTimeIT = new InstancePropertyTarget(freshNodeOf(InitialRuntime.initialTime))
      val tempSourceIT = new InstancePropertyTarget(freshNodeOf(TemporalSource.initialTime))

      val computeGenerator =
        if (initialTimeResolutionMode == "eager")
          new AlreadyCompletedNode(timeLookup(resolver))
        else {
          val provider = new SITweakValueProvider {
            // resolving initial time lazily and in background
            private lazy val initTime: Instant = timeLookup(resolver)
            override def valueOf(node: PropertyNode[_]): AnyRef = initTime
            override def isKeyDependent: Boolean = false
            override def serializeAsValue: Option[Any] = Some(initTime)
          }
          val initTimeComputer = new TweakValueProviderNode[Instant](provider, modify = false)
          if (initialTimeResolutionMode == "async") Scheduler.currentOrDefault.evaluateNodeAsync(initTimeComputer)
          // else we resolve it lazily (= only when people asks for it!)
          initTimeComputer
        }

      val tweakTemplate = new TweakNode(computeGenerator)
      Scenario(
        new Tweak(runTimeIT, tweakTemplate),
        new Tweak(tempSourceIT, tweakTemplate)
      )
    }
    scenario
  }

  // This cache can only hold one item.  Optimized for the frequent case where there is just one initial runtime.
  @volatile private var cachedInitialRuntimeScenarioStack: (ScenarioStack, Seq[Tweak]) = (null, Seq.empty[Tweak])

  /**
   * Extract original top scenario given current (non-top) scenario. HACK!!!!!!
   */
  override private[optimus] def initialRuntimeScenarioStack(ss: ScenarioStack, initialTime: Instant): ScenarioStack = {
    if (((initialTime eq null) && ss.isScenarioIndependent) || allowIllegalOverrideOfInitialRuntimeEnvironment)
      ss
    else {
      if (initialTime ne null) {
        val initTimeTweaks = Seq(
          SimpleValueTweak(InitialRuntime.initialTime)(initialTime),
          SimpleValueTweak(TemporalSource.initialTime)(initialTime))

        val (cachedSS, cachedInitTime) = cachedInitialRuntimeScenarioStack
        val reqCacheID = ss.ssShared._cacheID
        if ((cachedSS ne null) && cachedInitTime == initTimeTweaks && cachedSS.ssShared._cacheID == reqCacheID)
          cachedSS
        else {
          val newShared = ScenarioStackShared(ss.env, ss.siParams, null, uniqueID = false)
          val rtss = newShared.scenarioStack.createChild(Scenario(initTimeTweaks), this)
          newShared.setInitTimeScenarioStack(rtss)
          cachedInitialRuntimeScenarioStack = (rtss, initTimeTweaks)
          rtss
        }
      } else ss.initialRuntimeScenarioStack // take the real initialTime tweaks from the root
    }
  }
}
