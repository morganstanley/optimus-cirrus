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

import optimus.config.OptimusConfigurationException
import optimus.config.RuntimeConfiguration
import optimus.config.scoped.ScopedSchedulerPlugin
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils
import optimus.graph.NodeTaskInfo
import optimus.logging.OptimusLogging
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.annotations.deprecating
import optimus.platform.dal.ClientSideDSI
import optimus.platform.dal.DALEntityResolver
import optimus.platform.dal.RuntimeProperties
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation
import optimus.platform.dal.config.DalUri
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.session.InitSession
import optimus.platform.dal.session.RolesetMode
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DefaultContext
import optimus.platform.dsi.bitemporal.NamedContext
import optimus.platform.dsi.bitemporal.NamedContextType
import optimus.platform.dsi.bitemporal.SharedContext
import optimus.platform.dsi.bitemporal.SharedContextType
import optimus.platform.dsi.bitemporal.UniqueContext
import optimus.platform.dsi.bitemporal.UniqueContextType
import optimus.platform.inputs.loaders.FrozenNodeInputMap
import optimus.platform.inputs.loaders.OptimusLoaders
import optimus.platform.runtime._

import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicReference
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object OptimusTask extends InitSession {
  sealed trait FinalizationBehavior
  case object ShutdownAutomaticallyWhenUnused extends FinalizationBehavior
  case object KeepAlive extends FinalizationBehavior

  private def defaultScopedInputs: FrozenNodeInputMap = OptimusLoaders.defaults.frozenNodeInputMap

  private[optimus] def sysPropRoleset: Option[RolesetMode.SpecificRoleset] = {
    Option(RuntimeProperties.clientSessionRolesets)
      .filter(_.nonEmpty)
      .map(RolesetMode.SpecificRoleset.commaSeparated)
  }

  private[optimus] final def currentSessionRolesetMode: Option[RolesetMode.Established] = {
    EvaluationContext.entityResolver
      .asInstanceOf[DALEntityResolver]
      .dsi match {
      case c: ClientSideDSI =>
        Some(c.getSession().rolesetMode)
      case _ =>
        None
    }
  }

  // only exposed for migration purposes, please use currentSessionRolesetMode
  private[optimus] final def currentSessionRoles: Set[String] = {
    currentSessionRolesetMode
      .map {
        case RolesetMode.SpecificRoleset(rs)   => rs
        case RolesetMode.UseLegacyEntitlements => Set.empty[String]
      }
      .getOrElse(Set.empty[String])
  }

  private[optimus] def forceInitThread(
      components: RuntimeComponents,
      scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin] = null): ScenarioState =
    forceInitThread(components, defaultScopedInputs, scopedPlugins)

  private def forceInitThread(
      components: RuntimeComponents,
      scopedInputs: FrozenNodeInputMap,
      scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin]): ScenarioState = {
    // Initial GetInfo / Session establishment called during createRuntime
    // This will request the roleset if --allRoles was passed, or use the roles from the system or cmdline (if any)
    val scenarioState = components.createUntweakedScenarioState(scopedInputs, scopedPlugins)
    val runtime = scenarioState.environment
    try {
      EvaluationContext.initializeWithNewInitialTime(scenarioState)

      // TODO (OPTIMUS-21169): this path can be removed when all brokers support
      // resolving allRoles on the server-side during session establishment (EstablishAllRolesWithSession)
      runtime.entityResolver.reinitWithLocallyResolvedAllRoles(runtime.config)
      AdvancedUtils.currentScenarioState()
    } catch {
      case NonFatal(ex) =>
        runtime.entityResolver.close()
        throw ex
    }
  }

  def initThread(
      config: RuntimeConfiguration,
      asyncConfigOpt: Option[DalAsyncConfig] = None,
      cclOverride: Option[ClassLoader] = None,
      scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin] = null): ScenarioState =
    initThread(config, asyncConfigOpt, cclOverride, defaultScopedInputs, scopedPlugins)

  private def initThread(
      config: RuntimeConfiguration,
      asyncConfigOpt: Option[DalAsyncConfig],
      cclOverride: Option[ClassLoader],
      scopedInputs: FrozenNodeInputMap,
      scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin]): ScenarioState = {
    if (!EvaluationContext.isInitialised) {
      val components = cclOverride.fold(new RuntimeComponents(config, asyncConfigOpt))(
        new RuntimeComponents(config, asyncConfigOpt, _))
      forceInitThread(components, scopedInputs, scopedPlugins)
    } else AdvancedUtils.currentScenarioState()
  }
}

trait OptimusTask {
  import OptimusTask._
  OptimusLogging.install()

  protected[platform] lazy val scopedInputs = new AtomicReference[FrozenNodeInputMap](defaultScopedInputs)

  protected final def dalURI: Option[String] = dalLocation match {
    case _: DalEnv => None
    case u: DalUri => Some(u.underlying.toString)
  }

  protected final def envConfigName: Option[DalEnv] = dalLocation match {
    case d: DalEnv => Some(d)
    case _: DalUri => None
  }

  protected def dalLocation: DalLocation
  protected def dalContext: Context = DefaultContext
  protected def appId: DalAppId
  protected def zone: DalZoneId = DalZoneId.default
  protected def rolesetMode: RolesetMode = RolesetMode.Default
  protected def sessionTokenLocation: Option[String] = None
  protected def scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin] = null

  /** The context classloader to use for threads executing this task. Apps generally should not care about this. */
  protected def contextClassLoaderOverride: Option[ClassLoader] = None // hook for possible overrides

  protected def runtimeConfig: RuntimeConfiguration = {
    if (appId == null || appId.isUnknown) throw new IllegalArgumentException("appId is mandatory")
    GridProfilerUtils.setAppIdForCrumbs(appId.underlying)

    var conf = runtimeConfigurationFactory.getClientRuntimeConfigurationFromDalLocation(dalLocation, appId, zone)

    if (dalContext != DefaultContext) {
      val parameters = dalContext match {
        case DefaultContext      => ""
        case NamedContext(name)  => s"&context=${NamedContextType.id}&context.name=$name"
        case SharedContext(name) => s"&context=${SharedContextType.id}&context.name=$name"
        case UniqueContext(uuid) => s"&context=${UniqueContextType.id}&context.name=$uuid"
      }
      conf = conf.withOverride(
        RuntimeComponents.DsiUriProperty,
        s"${conf.get(RuntimeComponents.DsiUriProperty).mkString}$parameters")
    }

    val forceLdapEntitlements = RuntimeProperties.clientForceLdapEntitlements

    val runtimeRolesetMode: RolesetMode =
      if (forceLdapEntitlements) RolesetMode.UseLegacyEntitlements
      else OptimusTask.sysPropRoleset.getOrElse(rolesetMode)

    conf = conf.withOverride(RuntimeProperties.DsiSessionRolesetModeProperty, runtimeRolesetMode)

    sessionTokenLocation.foreach {
      case "-" =>
        val data = Source.stdin.bufferedReader()
        val tokBytes = Stream.continually(data.read()).map(_.toByte).takeWhile(_ != -1).toArray
        conf = conf.withOverride(RuntimeProperties.DsiOnBehalfSessionToken, tokBytes.toVector)
      case loc if loc.isEmpty => throw new OptimusConfigurationException("Provided DAL session token file is empty.")
      case loc =>
        if (!Files.exists(Paths.get(loc)))
          throw new OptimusConfigurationException(s"DAL session token file: $loc does not exist.")
        else
          conf =
            conf.withOverride(RuntimeProperties.DsiOnBehalfSessionToken, Files.readAllBytes(Paths.get(loc)).toVector)
    }

    val entityUpcastDomain = System.getProperty(RuntimeProperties.DsiEntityUpcastingProperty)
    conf = if (entityUpcastDomain ne null) {
      if (UpcastingUtil.isInvalidDomain(entityUpcastDomain))
        throw new OptimusConfigurationException(s"invalid upcast domain: $entityUpcastDomain")
      conf.withOverride(RuntimeProperties.DsiEntityUpcastingProperty, entityUpcastDomain)
    } else conf
    val eventUpcastDomain = System.getProperty(RuntimeProperties.DsiEventUpcastingProperty)
    conf = if (eventUpcastDomain ne null) {
      if (UpcastingUtil.isInvalidDomain(eventUpcastDomain))
        throw new OptimusConfigurationException(s"invalid upcast domain: $eventUpcastDomain")
      conf.withOverride(RuntimeProperties.DsiEventUpcastingProperty, eventUpcastDomain)
    } else conf
    conf
  }

  protected def runtimeConfigurationFactory: ClientRuntimeConfigurationFactoryBase = RuntimeConfigurationFactory

  lazy private[this] val shutdownAutomaticallyWhenNoLongerInUse =
    DiagnosticSettings.getBoolProperty("optimus.runtime.autoShutdown", true)
  // This is a workaround for a scala compiler bug
  lazy private val defaultFinalizationBehavior =
    if (shutdownAutomaticallyWhenNoLongerInUse) ShutdownAutomaticallyWhenUnused else KeepAlive

  protected[optimus] def scenarioState: ScenarioState = scenarioState(None)

  protected[optimus] def scenarioState(asyncConfigOpt: Option[DalAsyncConfig]): ScenarioState = {
    val env =
      if (!EvaluationContext.isInitialised)
        OptimusTask.synchronized {
          OptimusTask.initThread(
            runtimeConfig,
            asyncConfigOpt,
            contextClassLoaderOverride,
            scopedInputs.get,
            scopedPlugins)
        }
      else AdvancedUtils.currentScenarioState()
    env
  }

  private[optimus /*platform*/ ] def createRuntimeComponents(
      asyncConfigOpt: Option[DalAsyncConfig] = Some(DalAsyncBatchingConfig.default)): RuntimeComponents = {
    contextClassLoaderOverride.fold(new RuntimeComponents(runtimeConfig, asyncConfigOpt))(
      new RuntimeComponents(runtimeConfig, asyncConfigOpt, _))
  }

  @deprecating(suggestion = "Please find a structured way to manage optimus thread initialization. Do not use.")
  protected final def unsafeDoNotUseForceInitThread(
      asyncConfigOpt: Option[DalAsyncConfig] = Some(DalAsyncBatchingConfig.default)): Unit = {
    val components =
      contextClassLoaderOverride.fold(new RuntimeComponents(runtimeConfig))(
        new RuntimeComponents(runtimeConfig, asyncConfigOpt, _))
    OptimusTask.forceInitThread(components, scopedPlugins)
  }

  final def withOptimusRunnable(r: Runnable): Unit = {
    withOptimus(() => r.run())
  }

  @closuresEnterGraph
  final def withOptimus[A](
      f: () => A,
      asyncConfigOpt: Option[DalAsyncConfig] = Some(DalAsyncBatchingConfig.default),
      finalizationBehavior: FinalizationBehavior = defaultFinalizationBehavior): A = {
    val state = scenarioState(asyncConfigOpt)
    finalizationBehavior match {
      case ShutdownAutomaticallyWhenUnused => state.environment.doUsing { f() }
      case KeepAlive                       => f()
    }
  }

  final def withOptimusThread(
      r: Runnable,
      asyncConfigOpt: Option[DalAsyncConfig] = Some(DalAsyncBatchingConfig.default)): Thread = {
    withOptimusThread(() => r.run(), asyncConfigOpt)
  }

  @closuresEnterGraph
  private final def withOptimusThread(f: () => Unit, asyncConfigOpt: Option[DalAsyncConfig]): Thread = {
    val optScenarioState = if (EvaluationContext.isInitialised) Some(AdvancedUtils.currentScenarioState()) else None
    new Thread() {
      override def run(): Unit = {
        val state = optScenarioState.getOrElse(scenarioState(asyncConfigOpt))
        EvaluationContext.initialize(state)
        withOptimus(f, asyncConfigOpt, defaultFinalizationBehavior)
      }
    }
  }

}

abstract class JavaOptimusTask extends OptimusTask

// be careful when updating this implementation as it should be easily accessible from Java
/**
 * Defines Java-friendly implementation of the OptimusTask which allows use of Optimus environment in a Java process
 * Note, that at the moment multiple DAL connections are not supported from within a process, so multiple instances of
 * this class are allowed, but every subsequent instance of the class has to be created from within an Optimus-enabled
 * thread (apart from the very first one in the process)
 */
class OptimusTaskCls private[optimus /*platform*/ ] (
    override val appId: DalAppId,
    dalURI: Option[String] = None,
    envConfigName: Option[DalEnv] = None,
    override val rolesetMode: RolesetMode = RolesetMode.Default,
    override val zone: DalZoneId = DalZoneId.default)
    extends OptimusTask {
  // Java-friendly constructors
  def this(appId: String, dalUriArg: String) = this(DalAppId(appId), Option(dalUriArg), None)
  def this(appId: String, dalUriArg: String, envConfigNameArg: String) =
    this(DalAppId(appId), Option(dalUriArg), Option(DalEnv(envConfigNameArg)))
  def this(appId: String, dalUriArg: String, rolesetsArg: java.util.List[java.util.Set[String]]) =
    this(DalAppId(appId), Option(dalUriArg), None, OptimusTaskCls.rolesetsToScala(rolesetsArg))
  def this(appId: String, dalUriArg: String, rolesetsArg: java.util.List[java.util.Set[String]], zoneArg: String) =
    this(DalAppId(appId), Option(dalUriArg), None, OptimusTaskCls.rolesetsToScala(rolesetsArg), DalZoneId(zoneArg))
  def this(
      appId: String,
      dalUriArg: String,
      envConfigNameArg: String,
      rolesetsArg: java.util.List[java.util.Set[String]]) =
    this(
      DalAppId(appId),
      Option(dalUriArg),
      Option(DalEnv(envConfigNameArg)),
      OptimusTaskCls.rolesetsToScala(rolesetsArg))
  def this(
      appId: String,
      dalUriArg: String,
      envConfigNameArg: String,
      rolesetsArg: java.util.List[java.util.Set[String]],
      zoneArg: DalZoneId) =
    this(
      DalAppId(appId),
      Option(dalUriArg),
      Option(DalEnv(envConfigNameArg)),
      OptimusTaskCls.rolesetsToScala(rolesetsArg),
      zoneArg)

  override protected def dalLocation: DalLocation = DalLocation(envConfigName, dalURI)
}

object OptimusTaskCls {
  private def rolesetsToScala(rolesets: java.util.List[java.util.Set[String]]): RolesetMode =
    RolesetMode.fromSeqSetString(rolesets.asScala.map(_.asScala.toSet))
}

object OptimusTaskLog
