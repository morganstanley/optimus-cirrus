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
package optimus.buildtool.builders.postinstallers.apprunner

import java.util.concurrent.ConcurrentHashMap
import java.util.{Set => JSet}

import optimus.buildtool.builders.BackgroundAppId
import optimus.buildtool.builders.BackgroundProcessBuilder
import optimus.buildtool.builders.postinstallers.PostInstaller
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.WorkspaceSourceRoot
import optimus.buildtool.trace.PostInstallApp
import optimus.buildtool.utils.BlockingQueue
import optimus.buildtool.utils.Commit
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

/**
 * Runs applications for a specific scope after its installation. This is a "hack" that should be carefully used and
 * only for apps that are not RT.
 */
class PostInstallAppRunner(
    workspaceSourceRoot: WorkspaceSourceRoot,
    scopeConfigSource: ScopeConfigurationSource,
    installDir: Directory,
    installVersion: String,
    logDir: Directory,
    latestCommit: Option[Commit],
    bundleClassJars: Boolean
) extends PostInstaller
    with Log {
  // OPTIMUS-49684: OBT environment variables that are not forwarded to the postinstall app
  //
  // We specifically clean those variables because they will be set by the runconf for the PostInstallApp and should
  // never inherit the values from the OBT runconf.
  private val unsafeEnvVariables = Seq("JAVA_OPTS", "JAVA_HOME", "CLASSPATH", "OPTIMUSAPP_OPTS", "LD_PRELOAD")

  private val pathBuilder = InstallPathBuilder.dev(installDir, installVersion)
  protected val isWindows = Utils.isWindows

  private val args: Seq[String] = Seq(
    "--installDir",
    installDir.pathString,
    "--installVersion",
    installVersion,
    "--srcDir",
    workspaceSourceRoot.pathString,
    "--commit",
    latestCommit.map(_.hash).getOrElse("UNKNOWN")
  )

  object Tracker {
    private val _processedScopes: JSet[ScopeId] = ConcurrentHashMap.newKeySet()
    private val _scopesToProcess = new BlockingQueue[(ScopeId, JSet[ScopeId])]

    def scheduleForProcessing(scopeId: ScopeId, upstreams: JSet[ScopeId]): Unit =
      if (!_processedScopes.contains(scopeId)) _scopesToProcess.put(scopeId, upstreams)

    def pollToProcess(): Seq[ScopeId] = {
      val (ready, notReady) = _scopesToProcess.pollAll().partition { case (_, upstreams) =>
        _processedScopes.containsAll(upstreams)
      }
      notReady.foreach { case (scopeId, upstreams) => scheduleForProcessing(scopeId, upstreams) }
      ready.collect { case (scopeId, _) => scopeId }
    }

    def markAsProcessed(scopeId: ScopeId): Unit = _processedScopes.add(scopeId)

    def processedScopes() = _processedScopes.asScala

    def reset(): Unit = {
      _processedScopes.clear()
      _scopesToProcess.clear()
    }
  }

  @async override def postInstallFiles(scopeIds: Set[ScopeId], files: Seq[FileAsset], successful: Boolean): Unit =
    // We do not launch the apps if the build contains errors
    if (successful) scopeIds.foreach { id =>
      val dependencies = scopeConfigSource.scopeConfiguration(id).dependencies
      val upstreams = dependencies.compileDependencies.internal ++ dependencies.runtimeDependencies.internal
      Tracker.scheduleForProcessing(id, upstreams.toSet.asJava)
      // if we've got class jar bundling, then we can't trigger post install apps until the build has completed
      if (!bundleClassJars) triggerPostInstallApps()
    }

  @async override def postInstallFiles(files: Seq[FileAsset], successful: Boolean): Unit = {
    // if we've got class jar bundling, then we can't trigger post install apps until the build has completed
    if (bundleClassJars) triggerPostInstallApps()
    // run apps designed to run at very end of build
    Tracker.processedScopes().foreach(runPostInstallApps(_, afterInstall = true))
  }

  @async private def triggerPostInstallApps(): Unit =
    Tracker.pollToProcess().foreach(runPostInstallApps(_, afterInstall = false))

  @async private def runPostInstallApps(scopeId: ScopeId, afterInstall: Boolean): Unit = {
    val postInstallApps = scopeConfigSource.postInstallApps(scopeId).map(_.filter(_.afterInstall == afterInstall))
    // running outer apps in sequence
    postInstallApps.aseq.foreach { apps =>
      // running inner apps in parallel
      apps.apar.foreach { app =>
        val appLauncher = selectAppLauncher(scopeId, app.name)
        val cmd = Seq(appLauncher.pathString) ++ args ++ app.args
        // NOTE: We are not using the passed files to find the application launcher as it contains only the files
        // that we have incrementally installed, and we want to run these apps at every installation.
        launchProcess(scopeId, app.name, cmd)
      }
    }
    Tracker.markAsProcessed(scopeId)
    triggerPostInstallApps() // checking if other apps are ready to run
  }

  private def selectAppLauncher(id: ScopeId, appName: String): FileAsset = {
    val appLauncherName = if (isWindows) s"$appName.bat" else appName
    val appLauncherScript = pathBuilder.binDir(id).resolveFile(appLauncherName)
    def msg = s"""App launcher ${appLauncherScript.pathString} does not exist.
                 |Please, check that a runconf application named $appName exists for scope $id
                 |""".stripMargin.replaceAll("\n", "")
    require(exists(appLauncherScript), msg)
    appLauncherScript
  }

  protected def exists(appLauncherScript: FileAsset): Boolean = appLauncherScript.exists

  @async protected def launchProcess(scopeId: ScopeId, appName: String, cmd: Seq[String]): Unit = {
    val id = BackgroundAppId(scopeId, appName)
    log.info(s"[$id] Starting...")
    val (durationInNanos, _) = AdvancedUtils.timed {
      BackgroundProcessBuilder(id, id.logFile(logDir), cmd, envVariablesToClean = unsafeEnvVariables)
        .build(scopeId, PostInstallApp(appName), lastLogLines = 30)
    }
    log.info(s"[$id] Completed in ${Utils.durationString(durationInNanos / 1000000L)}")
  }

  @async override def complete(successful: Boolean): Unit = Tracker.reset()

}
