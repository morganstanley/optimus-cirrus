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
package optimus.graph.diagnostics.rtverifier

import msjava.slf4jutils.scalalog.getLogger
import optimus.core.MonitoringBreadcrumbs
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.JsonMapper
import optimus.graph.diagnostics.ReportOnShutdown

import java.lang.{StringBuilder => JStringBuilder}
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

object RTVerifierReporter extends RTVerifierReporter
trait RTVerifierReporter {
  private val log = getLogger(getClass)

  private val isEnabled = DiagnosticSettings.enableRTVerifier
  private val publishCrumbs = DiagnosticSettings.publishRTVerifierCrumbs
  private val writeReport = DiagnosticSettings.writeRTVerifierReport
  private val accumulateViolations = DiagnosticSettings.rtvAccumulateViolations
  private val violations: ListBuffer[Violation] = ListBuffer.empty

  /* use only for test purposes! */
  def flushViolations(): Seq[Violation] = {
    val vs = violations.clone()
    violations.clear()
    vs
  }

  private def extractModule(clazzName: String): String = {
    try {
      val clazz = Class.forName(clazzName)
      val jarLocation = clazz.getProtectionDomain.getCodeSource.getLocation.getPath
      extractModuleFromJar(jarLocation)
    } catch {
      case NonFatal(ex) => s"UNKNOWN:$clazzName:${ex.getMessage}" // returning full info, so that it is easier to debug!
    }
  }

  // Note: metas, bundles, modules and types can contain chars like "-" and "_"! so \w+ is not enough here
  private val name = "[^./]+"
  // example: .../build_obt/1.21/scala/optimus.platform.platform-test.test.INCR.HASH1234abcde.jar
  // OR: .../build_obt/1.21/scala/optimus.platform.platform-test.test.HASH1234abcde.jar
  private val LocalJarExtractor = raw""".*/($name\.$name\.$name\.$name)\..*\.jar$$""".r
  // examples: .../optimus/platform/local/install/common/lib/platform.jar
  // OR .../optimus/platform/local/install/common/lib/platform.test.jar
  private val InstalledJarExtractor = raw""".*/($name)/($name)/.*/install/common/$name/($name)\.?(.*)\.jar""".r
  // examples: .../optimus/PROJ/platform/common/lib/platform.jar
  // OR .../optimus/PROJ/platform/lib/platform.test.jar
  private val AFSJarExtractor = raw""".*/($name)/PROJ/($name)/.*/($name)\.?(.*)\.jar""".r
  protected def extractModuleFromJar(jarLocation: String): String = {
    def toPrettyScopeString(meta: String, bundle: String, module: String, tpe: String): String =
      Seq(meta, bundle, module, if (tpe.isEmpty) "main" else tpe).mkString(".")

    jarLocation match {
      case InstalledJarExtractor(meta, bundle, module, tpe) =>
        toPrettyScopeString(meta = meta, bundle = bundle, module = module, tpe = tpe)
      case AFSJarExtractor(meta, bundle, module, tpe) =>
        toPrettyScopeString(meta = meta, bundle = bundle, module = module, tpe = tpe)
      case LocalJarExtractor(module) => module
      case unparsableLocation => s"UNKNOWN:$unparsableLocation" // returning full info, so that it is easier to debug!
    }
  }

  def reportClassViolation(category: String, key: String, details: String, clazzName: String): Unit =
    reportViolation(category = category, key = key, details = details, owner = extractModule(clazzName))

  def reportAppletViolation(category: String, key: String, details: String, appletId: String): Unit =
    reportViolation(category = category, key = key, details = details, owner = appletId)

  private def reportViolation(category: String, key: String, details: String, owner: String): Unit =
    if (isEnabled) {
      val violation = Violation(category = category, key = key, details = details, owner = owner)

      // only accumulate violations if we need to write a report on shutdown
      // or if we have explicitly requested to do so (usually for test purposes)
      if (writeReport || accumulateViolations) violations.append(violation)

      if (publishCrumbs) MonitoringBreadcrumbs.sendRTViolationCrumb(violation)
      if (writeReport) enableReportOnShutdown

      // let's be overly defensive here, as we will detect RT violations without reporting them to the user
      if (!publishCrumbs && !writeReport) {
        log.warn("RT violations are detected but unreported as both written reports and crumbs are disabled")
      }
    }

  // this is lazy so that we register only once (i.e., when we find the first violation)
  private lazy val enableReportOnShutdown: Unit = ReportOnShutdown.register(suppliedDir = "")

  def rtViolationsContent: Option[String] = {
    if (violations.isEmpty || !writeReport) None
    else {
      val data: Map[String, Seq[FrameViolation]] =
        violations.groupBy(_.category).map { case (category, vs) =>
          category -> FrameViolation.convertAndSort(vs)
        }

      val sb = new JStringBuilder
      sb.append("rtViolations = ")
      sb.append(JsonMapper.mapper.writeValueAsString(data))
      Some(sb.toString)
    }
  }
}
