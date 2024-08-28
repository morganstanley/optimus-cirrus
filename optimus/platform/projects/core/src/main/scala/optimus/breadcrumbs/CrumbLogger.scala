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
package optimus.breadcrumbs

import msjava.slf4jutils.scalalog.Logger
import optimus.breadcrumbs.crumbs.{Crumb, LogCrumb}
import optimus.core.ChainedNodeID._
import optimus.platform.EvaluationContext

object CrumbLogger {

  final def runtimeID = EvaluationContext.scenarioStack.env.config.runtimeConfig.rootID.toString

  private def send(level: BreadcrumbLevel.Level, source: Crumb.Source, msg: String): Unit = {
    send(level, nodeID, source, msg)
  }

  private def send(level: BreadcrumbLevel.Level, uuid: ChainedID, source: Crumb.Source, msg: String): Unit = {
    Breadcrumbs(uuid, new LogCrumb(level.toString.toUpperCase, _, source, msg), level)
  }

  def isInfoEnabled = Breadcrumbs.isInfoEnabled(nodeID)
  def isDebugEnabled = Breadcrumbs.isDebugEnabled(nodeID)
  def isTraceEnabled = Breadcrumbs.isTraceEnabled(nodeID)

  def info(msg: => String) = LogCrumb.info(nodeID, Crumb.OptimusSource, msg)
  def error(msg: => String) = LogCrumb.error(nodeID, Crumb.OptimusSource, msg)
  def warn(msg: => String) = LogCrumb.warn(nodeID, Crumb.OptimusSource, msg)
  def debug(msg: => String) = LogCrumb.debug(nodeID, Crumb.OptimusSource, msg)
  def trace(msg: => String) = LogCrumb.trace(nodeID, Crumb.OptimusSource, msg)

  def info(source: Crumb.Source, msg: => String) = LogCrumb.info(nodeID, source, msg)
  def error(source: Crumb.Source, msg: => String) = LogCrumb.error(nodeID, source, msg)
  def warn(source: Crumb.Source, msg: => String) = LogCrumb.warn(nodeID, source, msg)
  def debug(source: Crumb.Source, msg: => String) = LogCrumb.debug(nodeID, source, msg)
  def trace(source: Crumb.Source, msg: => String) = LogCrumb.trace(nodeID, source, msg)

  def info(log: Logger, uuid: ChainedID, msg: => String) =
    if (Breadcrumbs.collecting && log.isInfoEnabled())
      /* legitimate use of isXEnabled */ send(BreadcrumbLevel.Info, uuid, Crumb.OptimusSource, msg)
  def error(log: Logger, uuid: ChainedID, msg: => String) =
    if (Breadcrumbs.collecting && log.isErrorEnabled())
      /* legitimate use of isXEnabled */ send(BreadcrumbLevel.Error, uuid, Crumb.OptimusSource, msg)
  def warn(log: Logger, uuid: ChainedID, msg: => String) =
    if (Breadcrumbs.collecting && log.isWarnEnabled()) send(BreadcrumbLevel.Warn, uuid, Crumb.OptimusSource, msg)
  def debug(log: Logger, uuid: ChainedID, msg: => String) =
    if (Breadcrumbs.collecting && log.isDebugEnabled())
      /* legitimate use of isXEnabled */ send(BreadcrumbLevel.Debug, uuid, Crumb.OptimusSource, msg)
  def trace(log: Logger, uuid: ChainedID, msg: => String) =
    if (Breadcrumbs.collecting && log.isTraceEnabled())
      /* legitimate use of isXEnabled */ send(BreadcrumbLevel.Trace, uuid, Crumb.OptimusSource, msg)

}
