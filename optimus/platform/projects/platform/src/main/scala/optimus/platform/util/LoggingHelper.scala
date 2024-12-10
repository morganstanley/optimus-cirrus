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

import ch.qos.logback.classic.{Logger => LogbackLogger}
import ch.qos.logback.core.spi.AppenderAttachable
import optimus.platform.PackageAliases
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

private[optimus] /*[platform]*/ object LoggingHelper {
  private val toleratedSyncLoggers: Set[String] = Set("com.msdw.dpg.eticket.infra.util.logging.back.C2AsyncAppender")

  def allLoggers: Seq[LogbackLogger] = {
    LoggerFactory.getILoggerFactory match {
      case lc: ch.qos.logback.classic.LoggerContext => lc.getLoggerList.asScala
      case _                                        => Nil // Non-logback logging frameworks are ignored
    }
  }

  def syncLoggers: Seq[LogbackLogger] = allLoggers.filterNot(checkAsyncEnabled)

  def checkAsyncEnabled(tree: AppenderAttachable[_]): Boolean = {
    tree.iteratorForAppenders.asScala.forall {
      case _: ch.qos.logback.core.AsyncAppenderBase[_] /* | _: msjava.logbackutils.async.AsyncAppender[_] */ => true
      case a if toleratedSyncLoggers.contains(a.getClass.getName)                                      => true
      case a: ch.qos.logback.core.spi.AppenderAttachable[_] => checkAsyncEnabled(a)
      case _                                                => false
    }
  }

  // Copies logging rules for $oldName.foo.bar to $newName.foo.bar so that rules defined for an old package will work for a
  // new package without the need to modify all logback.xml files at once.
  // TODO (OPTIMUS-36426): remove once all logback configs are updated to new package names
  def mangleLoggerNames(): Unit = PackageAliases.aliasesOldToNew.foreach { case (oldName, newName) =>
    allLoggers.map(l => l.getName -> l.getLevel).foreach {
      case (name, level) if name startsWith oldName =>
        LoggerFactory.getILoggerFactory.getLogger(newName + name.stripPrefix(oldName)) match {
          case logbackLogger: LogbackLogger => logbackLogger.setLevel(level)
          case _                            =>
        }
      case _ =>
    }
  }
}
