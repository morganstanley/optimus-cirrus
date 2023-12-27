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

  def getJavaOpts(maxLen: Int = 2000): String = {
    Option(System.getenv("JAVA_OPTS"))
      .map(_.trim)
      .map { o =>
        // the --add-exports / --add-opens are generally uninformative and waste room in our 2k limit
        // drop args after we reach 2000 chars
        val withoutJigsaw = o.split(" ").filterNot(_ startsWith "--add-").mkString(" ")
        if (withoutJigsaw.length > maxLen) withoutJigsaw.substring(0, maxLen - 3) + "..." else withoutJigsaw
      }
      .orNull
  }

  lazy val getHostInetAddr: InetAddress = InetAddress.getLocalHost
  lazy val getHost: String = getHostInetAddr.getHostName
  lazy val getUser: String = System.getProperty("user.name", "unknown")
  lazy val pid: Long = Pid.pidOrZero()
  lazy val gsfControllerId: Option[String] = Option(System.getenv("GSF_CONTROLLER_ID"))

  lazy val appName = Option(System.getProperty("optimus.ui.appname", System.getProperty("APP_NAME")))
  lazy val appVersion = Option(System.getProperty("optimus.ui.appver")).getOrElse("")
  lazy val os = Option(System.getProperty("os.name")).getOrElse("")
  lazy val tmId = sys.env.getOrElse("TREADMILL_INSTANCEID", "")
}

object HardwareInfo {
  private var id = -1
  lazy val allCpuInfo: Try[Map[Int, Map[String, String]]] = Try {
    val cpus = mutable.HashMap.empty[Int, mutable.HashMap[String, String]]
    readInfo("/proc/cpuinfo") { (k, v) =>
      if (k == "processor") {
        id = v.toInt
        cpus += id -> mutable.HashMap.empty[String, String]
      } else cpus(id) += k.replaceAllLiterally(" ", "_") -> v
    }
    cpus.toMap.mapValuesNow(_.toMap)
  }

  // TODO (OPTIMUS-51948): Consider making extracted info part of DTC key
  lazy val cpuInfo: Map[String, String] =
    if (Properties.isWin) Map("exception" -> "windows")
    else
      allCpuInfo.flatMap(infos => Try(infos(0) + ("processors" -> (id + 1).toString))) match {
        case Success(map) => map
        case Failure(t)   => Map("exception" -> t.toString)
      }

  /**
   * Intel sandybridge architecture detection based on cpu_family and cpu model according to:
   * https://en.wikichip.org/wiki/intel/cpuid
   */
  lazy val isSandyBridge =
    HardwareInfo.cpuInfo.getOrElse("model_name", "").contains("Intel") &&
      HardwareInfo.cpuInfo.get("cpu_family") == Some("6") &&
      (HardwareInfo.cpuInfo.get("model") == Some("45") || HardwareInfo.cpuInfo.get("model") == Some(
        "42"
      ))

  lazy val memInfo: Map[String, String] =
    if (Properties.isWin) Map("exception" -> "windows")
    else
      Try {
        val memInfos = mutable.HashMap.empty[String, String]
        readInfo("/proc/meminfo") { (k, v) => memInfos.put(k, v) }
        memInfos.toMap
      } match {
        case Success(map) => map
        case Failure(t)   => Map("exception" -> t.toString)
      }

  private def readInfo(file: String)(f: (String, String) => Unit): Unit = {
    val source = Source.fromFile(file)
    val it = source.getLines()
    while (it.hasNext) {
      val line = it.next()
      // if it's a key-value pair, parse it
      if (line.contains(":")) {
        val (k0, v0) = keyValue(line)
        f(k0, v0)
      }
    }
    source.close()
  }

  // parse : separated pairs in cpuinfo and meminfo files
  private def keyValue(line: String): (String, String) = {
    val colonIndex = line.indexOf(":")
    val k = line.take(colonIndex).trim
    val v = line.takeRight(line.length - colonIndex - 1).trim
    (k, v)
  }
}
