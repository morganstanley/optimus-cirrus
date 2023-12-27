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
package optimus.buildtool.bsp

import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.CancellationException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import ch.epfl.scala.bsp4j.BuildClient
import com.google.common.util.concurrent.ThreadFactoryBuilder
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.app.IncrementalMode
import optimus.buildtool.app.ScopedCompilationFactory
import optimus.buildtool.builders.StandardBuilder
import optimus.buildtool.builders.TrackedWorkspace
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.config.ScalaVersionConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.LocalDirectoryFactory
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.resolvers.IvyResolver
import optimus.buildtool.rubbish.ArtifactRecency
import optimus.buildtool.scope.FingerprintHasher
import optimus.buildtool.trace.ObtTraceListener
import optimus.buildtool.trace.TraceFilter
import optimus.buildtool.utils.GitLog
import optimus.platform._
import org.eclipse.lsp4j.jsonrpc.Launcher

import scala.collection.immutable.Seq
import scala.concurrent.TimeoutException

object BuildServerProtocolServer {
  private[bsp] val serverNumber = new AtomicInteger(1)
}

class BuildServerProtocolServer(
    workspace: TrackedWorkspace,
    builder: NodeFunction0[StandardBuilder],
    scopeFactory: ScopedCompilationFactory,
    workspaceName: String,
    workspaceRoot: Directory,
    workspaceSourceRoot: Directory,
    buildDir: Directory,
    outputDir: Directory,
    stdin: InputStream,
    stdout: OutputStream,
    clientInitializationTimeoutMs: Int,
    sendCrumbs: Boolean,
    installerFactory: NodeFunction1[Option[Directory], PostBuilder],
    scalaVersionConfig: NodeFunction0[ScalaVersionConfig],
    pythonEnabled: NodeFunction0[Boolean],
    ivyResolvers: NodeFunction0[Seq[IvyResolver]],
    directoryFactory: LocalDirectoryFactory,
    dependencyCopier: DependencyCopier,
    incrementalMode: IncrementalMode,
    hasher: FingerprintHasher,
    gitLog: Option[GitLog],
    gitAwareMessages: Boolean,
    listeners: List[ObtTraceListener] = Nil,
    traceFilter: Option[TraceFilter],
    recency: Option[ArtifactRecency],
    osVersion: String
) {
  private val log = getLogger(this)

  def runServer(): Boolean = {
    log.info(s"Starting server")

    val localService: BuildServerProtocolService = new BuildServerProtocolService(
      workspaceName = workspaceName,
      workspaceRoot = workspaceRoot,
      workspaceSourceRoot = workspaceSourceRoot,
      buildDir = buildDir,
      outputDir = outputDir,
      sendCrumbs = sendCrumbs,
      workspace = workspace,
      builder = builder,
      installerFactory = installerFactory,
      scalaVersionConfig = scalaVersionConfig,
      pythonEnabled = pythonEnabled,
      ivyResolvers = ivyResolvers,
      directoryFactory = directoryFactory,
      dependencyCopier = dependencyCopier,
      incrementalMode = incrementalMode,
      hasher = hasher,
      gitLog = gitLog,
      gitAwareMessages = gitAwareMessages,
      listeners = listeners,
      traceFilter = traceFilter,
      recency = recency,
      osVersion = osVersion
    )

    val launcher = new Launcher.Builder[BuildClient]()
      .setRemoteInterface(classOf[BuildClient])
      .setLocalService(localService)
      .setInput(stdin)
      .setOutput(stdout)
      .setExecutorService(
        Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
            .setNameFormat(
              s"BuildServerProtocolServer-${BuildServerProtocolServer.serverNumber.getAndIncrement()}-reader-%d"
            )
            .build()
        )
      )
      .create()

    localService.onConnectWithClient(launcher.getRemoteProxy)
    localService.listening = Some(launcher.startListening())

    try {
      // Wait for clientInitializationTimeoutMs. Note that other requests/responses after initialization can also happen
      // during this time - this is just a convenient way of using this thread to detect an init timeout
      try localService.listening.foreach(_.get(clientInitializationTimeoutMs, TimeUnit.MILLISECONDS))
      catch {
        case _: TimeoutException => // completely fine - just means that we didn't exit already
      }

      // by now, we should have initialized (and hopefully done a lot of other stuff too)
      if (localService.isSessionInitialized) {
        // wait for the listener to complete (either due to an exit request or because the client just went away,
        // e.g. IntelliJ was killed)
        localService.listening.foreach(_.get())
      } else {
        // we timed out without initializing, so die to avoid hanging around forever
        log.error("Timed out waiting for client to initialize - stopping")
        localService.listening.foreach(_.cancel(true))
      }
    } catch {
      case _: CancellationException => log.warn("BSP Listener was cancelled - stopping")
    } finally {
      stdin.close()
      stdout.close()
    }

    true
  }

}
