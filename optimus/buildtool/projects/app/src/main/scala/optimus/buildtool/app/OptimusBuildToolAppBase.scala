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

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.cache.NodeCaching
import optimus.buildtool.utils.FlexibleBooleanOptionHandler
import optimus.platform.entersGraph
import optimus.platform._
import optimus.platform.OptimusApp.ExitHandler
import optimus.platform.OptimusAppCmdLine
import org.kohsuke.args4j.CmdLineParser

private[buildtool] trait OptimusBuildToolAppBase[A <: OptimusAppCmdLine] extends OptimusAppTrait[A] {
  protected val log: Logger = getLogger(this)

  override protected def parseCmdline(args: Array[String], exitHandler: ExitHandler): Unit = {
    CmdLineParser.registerHandler(classOf[Boolean], classOf[FlexibleBooleanOptionHandler])
    super.parseCmdline(args, exitHandler)
  }

  @entersGraph override def preRun(): Unit = {
    super.preRun()
    // some annoying bit of code somewhere is setting this to true on startup, but we need it to be false else it
    // overrides the -usejavacp:false setting (see ZincInputs#scalacOptions to understand why we need that).
    System.setProperty("scala.usejavacp", "false")

    OptimusBuildToolImpl.info.properties.foreach(_.setCustomCache(NodeCaching.reallyBigCache))
  }

  protected def obtExit(ok: Boolean): Unit = {
    // the logic around suspend and exit codes in the default OptimusApp is incomprehensible spaghetti but presumably
    // many apps rely on it. So rather than changing it and risking breaking them, I've rolled my own
    if (exitMode == ExitMode.Suspend) {
      log.warn("Exit mode is Suspend - Suspending")
      suspend()
    } else {
      // This calls into parts of the OptimusApp shutdown sequence to ensure that crumbs are flushed and profiles saved and all that
      flushBeforeExit()
      exit(None, if (ok) 0 else 1)
    }
  }
}
