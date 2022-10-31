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

import scala.jdk.CollectionConverters._
import java.net.InetAddress
import optimus.platform.util.Log

import scala.collection.mutable
import scala.io.Source
import optimus.scalacompat.collection._
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import scala.util.Properties
import scala.util.control.NonFatal

object Pid extends Log {

  private def complain(e: Throwable) = {
    log.warn(
      "Unable to resolve current process id (pid) for troubleshooting or monitoring. Please fix immediately. Exception was {} '{}'.",
      e.getClass().getCanonicalName(),
      e.getMessage()
    )
    None
  }
  // TODO (OPTIMUS-47224): This should really return Option[Long], but we need to ensure it doesn't break
  // DAL code.
  val pidInt: Option[Int] =
    try {
      Some(ProcessHandle.current().pid().toInt)
    } catch {
      case NonFatal(e) => complain(e)
    }

  val pid: Option[Long] =
    try {
      val p = ProcessHandle.current().pid
      log.info(s"pid: $p") // matching the format that used to be in Pid.scala
      Some(p)
    } catch {
      case NonFatal(e) => complain(e)
    }

  def pidOrZero(): Long = pid.getOrElse(0);
}

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

  def getLogFiles(log: ch.qos.logback.core.spi.AppenderAttachable[_]): collection.Seq[String] = {
    val appenders = log.iteratorForAppenders().asScala
    val files = appenders.flatMap {
      case a: ch.qos.logback.core.spi.AppenderAttachable[_] => getLogFiles(a)
      case a: ch.qos.logback.core.FileAppender[_]           => Seq(a.getFile)
      case a: ch.qos.logback.core.ConsoleAppender[_]        => collection.Seq("console")
      case a                                                => Seq(a.toString)
    }
    files.toSeq
  }

  // I'm worried that this might be huge for some apps, causing the crumb to exceed the size limit and get dropped
  def getJavaOpts(): String = {
    Option(System.getenv("JAVA_OPTS"))
      .map(_.trim)
      .map { o =>
        // the --add-exports / --add-opens are generally uninformative and waste room in our 2k limit
        // drop args after we reach 2000 chars
        val withoutJigsaw = o.split(" ").filterNot(_ startsWith "--add-").mkString(" ")
        if (withoutJigsaw.length > 2000) withoutJigsaw.substring(0, 1997) + "..." else withoutJigsaw
      }
      .orNull
  }

  lazy val getHostInetAddr: InetAddress = InetAddress.getLocalHost
  lazy val getHost: String = getHostInetAddr.getHostName
  lazy val getUser: String = System.getProperty("user.name", "unknown")
  lazy val pid: Long = Pid.pidOrZero()
  lazy val gsfControllerId: Option[String] = Option(System.getenv("GSF_CONTROLLER_ID"))
}

object HardwareInfo {
  private val MapRe = """(\S.*\S)\s*\:\s*(.*\S)\s*""".r
  private var id = -1
  lazy val allCpuInfo: Try[Map[Int, Map[String, String]]] = Try {
      val cpus = mutable.HashMap.empty[Int, mutable.HashMap[String, String]]
      Source.fromFile("/proc/cpuinfo").getLines().foreach {
        case MapRe("processor", sid) =>
          id = sid.toInt
          cpus += id -> mutable.HashMap.empty
        case MapRe(k, v) if id >= 0 =>
          cpus(id) += k.replaceAllLiterally(" ", "_") -> v
        case _ =>
      }
      cpus.toMap.mapValuesNow(_.toMap)
  }

  // TODO (OPTIMUS-51948): Consider making extracted info part of DTC key
  lazy val cpuInfo: Map[String, String] = if(Properties.isWin) Map("exception" -> "windows") else
    allCpuInfo.flatMap(infos => Try(infos(0) + ("processors" -> (id+1).toString))) match {
      case Success(map) => map
      case Failure(t) => Map("exception" -> t.toString)
    }
  lazy val memInfo: Map[String, String] = if(Properties.isWin) Map("exception" -> "windows") else Try {
    Source.fromFile("/proc/meminfo").getLines.collect {
      case MapRe(k, v) => k -> v
    }.toMap
  } match {
    case Success(map) => map
    case Failure(t) => Map("exception" -> t.toString)
  }
}

