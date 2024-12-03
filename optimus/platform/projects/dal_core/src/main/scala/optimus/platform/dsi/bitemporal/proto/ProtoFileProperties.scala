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
package optimus.platform.dsi.bitemporal.proto

import optimus.platform.dal.ServerInfo

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern
import scala.util.control.NonFatal

object ProtoFileProperties { props =>
  /*
   * This stands for the version of dsi.proto file. It doesn't mean you need to increase this number each time you make some changes
   * but only need to increase this number if you want to handle the version difference at broker side. In runtime the DAL client will send
   * this number to broker and broker can apply some logic if needed (via DSIRequestInfo.clientProtoFileVerNum and ClientCapabilities)
   */
  // TODO (OPTIMUS-11744): this should be removed in favour of SupportedFeatures once all brokers support it
  final val versionNumber = 2

  // This version should be bumped up if there is a Broker->DDC regression or vice versa.
  final val ddcRegrBuild = 0

  final lazy val featureInfo = {
    // default info which would be sent. Please update both the build number and the date
    val defInfo = FeatureInfo(build = 111, "2024-11-07-0")

    def safeDo[R](f: => Option[R]): Option[R] = try { f }
    catch { case _: Throwable => None }

    def getOptVersion(s: String) = {
      safeDo {
        getVersion(s).map(FeatureInfo(build = defInfo.build, _))
      }
    }

    // try to retrieve optimus build number from AFS path
    safeDo {
      val r = Seq(
        safeDo(Option(System.getProperty("sun.java.command"))),
        safeDo {
          (Option(props.getClass.getProtectionDomain.getCodeSource)) map { _.getLocation.toString }
        }).flatMap { _.map { getOptVersion } toSeq }.find { _.isDefined }.flatten
      r
    } getOrElse { defInfo }
  }

  final val NO_FEATURE_INFO = new FeatureInfo(build = 0, "1957-11-04-0")

  // try to retrieve optimus build number from AFS path
  private[proto] def getVersion(s: String): Option[String] = {
    val versionPattern = Pattern.compile(""".*(p[a-z]{4}|platform)[/\\]+(\S+)[/\\]+common.*""")
    val matcher = versionPattern.matcher(s)
    if (matcher.matches) Some(matcher.group(2)) else None
  }
}

final case class FeatureInfo(build: Int, infoTag: String) extends ServerInfo {
  private val datePattern = Pattern.compile(".*[-]([\\d]{8})[-].*")

  def isDefined: Boolean = build > 0

  private def date(s: String) = {
    val matcher = datePattern.matcher(s)
    if (matcher.matches) {
      try {
        Some(LocalDate.parse(matcher.group(1), DateTimeFormatter.ofPattern("yyyyMMdd")))
      } catch {
        case NonFatal(_) => None
      }
    } else None
  }

  def getDate(): Option[LocalDate] = date(infoTag)

  def daysDiff(that: ServerInfo): Option[Long] = {
    val our = date(infoTag)
    val th = date(that.infoTag)
    for (o <- our; t <- th) yield o.toEpochDay - t.toEpochDay
  }

}
