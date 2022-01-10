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
package optimus.logging

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaIteratorConverter
import java.net.InetAddress

object LoggingInfo {
  def getLogFile: String = {
    org.slf4j.LoggerFactory.getILoggerFactory() match {
      case lc: ch.qos.logback.classic.LoggerContext =>
        val loggers = lc.getLoggerList.asScala
        loggers.flatMap(getLogFiles(_)).mkString(",")
      case x =>
        "unknown_context"
    }
  }

  def getLogFiles(log: ch.qos.logback.core.spi.AppenderAttachable[_]): Seq[String] = {
    val appenders = log.iteratorForAppenders().asScala
    val files = appenders.flatMap {
      case a: ch.qos.logback.core.spi.AppenderAttachable[_] => getLogFiles(a)
      case a: ch.qos.logback.core.FileAppender[_]           => Seq(a.getFile)
      case a: ch.qos.logback.core.ConsoleAppender[_]        => Seq("console")
      case a                                                => Seq(a.toString)
    }
    files.toSeq
  }

  lazy val getHostInetAddr: InetAddress = InetAddress.getLocalHost
  lazy val getHost: String = getHostInetAddr.getHostName
  lazy val getUser: String = System.getProperty("user.name", "unknown")

}
