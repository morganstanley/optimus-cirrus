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
package optimus.buildtool.app

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import ch.qos.logback.{classic => logback}
import ch.qos.logback.classic.util.ContextInitializer
import optimus.stratosphere.utils.ProcessUtils
import com.ms.zookeeper.clientutils.ZkEnv
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.CrumbHint
import optimus.breadcrumbs.crumbs.CrumbHints
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.trace.ObtCrumbSource
import optimus.platform.util.Log
import optimus.platform.breadcrumbs.BreadcrumbsSetup
import optimus.platform.breadcrumbs.BreadcrumbsSetup.Flags
import org.slf4j

import java.nio.file.Files

object OptimusBuildToolBootstrap extends Log {
  // Note: This timestamp format is chosen to match the format of '%t' for -Xloggc
  private[buildtool] val logFilePrefix: String = generateLogFilePrefix()

  def generateLogFilePrefix() = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").format(LocalDateTime.now)

  def initLogging(logDir: Directory, debug: Boolean, bspDebug: Boolean): Unit = {
    Files.createDirectories(logDir.path)
    slf4j.LoggerFactory.getILoggerFactory match {
      case lc: logback.LoggerContext =>
        def undefined(key: String): Boolean = sys.props.get(key).isEmpty && sys.env.get(key).isEmpty

        if (undefined("LOG_DIR_OVERRIDE"))
          sys.props.put("LOG_DIR_OVERRIDE", logDir.toString)
        if (undefined("LOG_FILE_PREFIX_OVERRIDE"))
          sys.props.put("LOG_FILE_PREFIX_OVERRIDE", logFilePrefix)
        if (debug && undefined("STDOUT_THRESHOLD_LEVEL_OVERRIDE"))
          sys.props.put("STDOUT_THRESHOLD_LEVEL_OVERRIDE", "DEBUG")
        if (bspDebug && undefined("BSP_IO_LOG_LEVEL_OVERRIDE"))
          sys.props.put("BSP_IO_LOG_LEVEL_OVERRIDE", "DEBUG")

        lc.reset()
        new ContextInitializer(lc).autoConfig()
      case f =>
        log.debug(s"Not overriding log dir for factory $f")
    }
    ProcessUtils.setupTerminal()
  }

  def initializeCrumbs(
      enableCrumbs: Boolean,
      obtBenchmarkScenario: Option[String] = None,
      sendLongTermBreadcrumbs: Boolean = false
  ): Unit =
    if (enableCrumbs) {
      BreadcrumbsSetup.initializeBreadcrumbsEnv(ZkEnv.qa, "obt", "OptimusBuildTool", Flags.DoNotInitializeEnv)

      val possiblyUseful = Seq(
        "BUILD_ID",
        "BUILD_NUMBER",
        "BUILD_URL",
        "STRATOSPHERE_INFRA",
        "JAVA_HOME",
        "JOB_BASE_NAME",
        "BUILD_TAG",
        "JENKINS_INSTANCE",
        "BRANCH_TO_BUILD",
        "PULL_REQUEST_ID",
        "ID_KVM",
        "SYS_LOC",
        "SYS_CAMPUS",
        "SYS_ENVIRONMENT",
        // these might exist on windows
        "COMPUTERTYPE",
        "OS"
      ) ++ StaticConfig.stringSeq("breadcrumbProperties")
      val niceEnv = for {
        k <- possiblyUseful
        v <- Option(System.getenv(k))
      } yield k -> v

      val elems = Seq(
        Properties.sysEnv -> niceEnv.toMap,
        Properties.obtCategory -> "BuildEnvironment") ++ obtBenchmarkScenario.map(Properties.obtBenchmarkScenario -> _)

      Breadcrumbs.info(
        ChainedID.root,
        PropertiesCrumb(
          _,
          ObtCrumbSource,
          if (sendLongTermBreadcrumbs) CrumbHints.LongTerm else Set.empty[CrumbHint],
          elems: _*)
      )
    }

}
