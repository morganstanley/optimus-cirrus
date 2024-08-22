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
package optimus.platform.util

import optimus.logging.LoggingInfo
import optimus.platform.EvaluationContext
import optimus.platform.dal.RuntimeProperties
import java.net.InetAddress

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.ChainedID
import spray.json._

/**
 * Metadata: collect and log this info for further log analysis (AppID,User,LogFile,HostName)
 */
object LogIDMetadata {

  private val MetadataKey = "LogIDMetaData"
  private val log = getLogger(this.getClass)
  private val host = InetAddress.getLocalHost.getHostName
  private val user = System.getProperty("user.name", "userid")

  private def appID: String = {
    val configOpt = Option(EvaluationContext.scenarioStack.env.config)
    configOpt.flatMap(_.runtimeConfig.get(RuntimeProperties.DsiAppIdProperty)).getOrElse("").toString
  }

  def asJSON(chainedID: ChainedID): String = {
    val json = JsObject(
      "AppID" -> JsString(appID),
      "User" -> JsString(user),
      "LogFile" -> JsString(LoggingInfo.getLogFile),
      "Host" -> JsString(host),
      "ChainedID" -> JsString(chainedID.prettyPrint)
    )

    s"[$MetadataKey: ${json.toString()}]"
  }

  def printAsJSON(chainedID: ChainedID): Unit =
    log.info(asJSON(chainedID))

  // Please notify regressionrunner-dev when altering this pattern.
  val chainedIdPattern = s""".*?\\[$MetadataKey: .*?"ChainedID" *: *"(.+?)(#[\\d#]+)?".*""".r

}
