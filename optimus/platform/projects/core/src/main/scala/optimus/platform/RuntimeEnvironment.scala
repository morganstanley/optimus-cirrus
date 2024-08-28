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
package optimus.platform

import java.io.PrintWriter
import java.io.StringWriter
import java.time.Instant
import optimus.platform.dal.EntityResolver
import optimus.config.RuntimeComponents
import msjava.slf4jutils.scalalog._
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.EventCrumb
import optimus.breadcrumbs.crumbs.Events
import optimus.breadcrumbs.crumbs.Crumb.RuntimeSource
import optimus.graph.NodeTask
import optimus.platform.RuntimeEnvironment.KnownNames
import optimus.platform.util.PrettyStringBuilder

import scala.util.Try
import scala.util.control.NonFatal

object RuntimeEnvironment {
  val minimal = new RuntimeEnvironment(null, null)
  private val NewLine = System.lineSeparator

  val KnownNames = RuntimeEnvironmentKnownNames

  private[optimus /*platform*/ ] def waiterNodeStack(n: NodeTask, maxLevel: Option[Int]): String = {
    val sb = new PrettyStringBuilder()
    n.waitersToFullMultilineNodeStack(false, sb)
    maxLevel match {
      case Some(level) => sb.toString.linesIterator.take(level).mkString(NewLine)
      case None        => sb.toString
    }
  }

  def messageWithStacks(message: String, exception: Throwable, nodeStackSize: Option[Int] = None): String = {
    val stringWriter = new StringWriter
    stringWriter.append(message)
    stringWriter.append(s"$NewLine because: ")
    exception.printStackTrace(new PrintWriter(stringWriter)) // this has tailing new line
    stringWriter.append(s" with Node Stack:$NewLine")
    stringWriter.append(
      s"${waiterNodeStack(EvaluationContext.currentNode, nodeStackSize)}"
    ) // this has tailing new line
    stringWriter.toString
  }

  def getAppId(ntsk: NodeTask): Option[String] = for {
    ss <- Option(ntsk.scenarioStack())
    env <- Option(ss.env)
    config <- Option(env.config.runtimeConfig)
    appId <- config.getString("optimus.dsi.appid")
  } yield appId
}

class RuntimeEnvironment private[optimus] (
    val config: RuntimeComponents,
    val entityResolver: EntityResolver
) extends ShutdownLifeCycle {
  addShutdownAction { Breadcrumbs(id, new EventCrumb(_, RuntimeSource, Events.RuntimeShutDown)) }
  if (entityResolver ne null) addShutdownAction { entityResolver.close() }

  if ((entityResolver ne null) && (config ne null) && config.envName != KnownNames.EnvNone)
    entityResolver.init(config)

  lazy val id: ChainedID = {
    val ret =
      if ((config ne null) && (config.runtimeConfig ne null) && config.envName != KnownNames.EnvNone)
        config.runtimeConfig.rootID
      else ChainedID.create()
    Breadcrumbs(ret, new EventCrumb(_, RuntimeSource, Events.RuntimeCreated))
    ret
  }

  // Won't get to the actual method for three more hops.  This indirection is necessary in order to avoid exposing
  // config directly, and to allow calling without depending on platform.
  private[optimus] def initialRuntimeScenarioStack(ss: ScenarioStack, initialTime: Instant) =
    config.initialRuntimeScenarioStack(ss, initialTime)

  private[optimus] def classLoader: ClassLoader =
    if (config ne null) config.contextClassloader else classOf[RuntimeEnvironment].getClassLoader
}

/**
 * Provides basic coordination around shutdown life-cycle, to whoever mixes it in.
 *
 * NOTE: This is very basic life-cycle support. If we need more we might want to look at existing Java / Scala libs that
 * give config and life-cycle.
 *
 * You can think of ShutdownLifeCycle as giving something similar to Java 7 AutoClosable, that is extended to cater for
 * more than one thread using the component.
 *
 *   - Shutdown co-ordination:
 *     - Preventing shutdown whilst things need the component
 *       - For RuntimeEnvironment this is the union of the lifetimes of the main thread, and or when GUI windows are
 *         open the GUI thread.
 *     - Registration of shutdown actions to be run when shutdown happens
 *       - For RuntimeEnvironment this is stopping the DSI actor
 *     - dispose() to force shutdown, even if something is using the component
 *       - For RuntimeEnvironment this used in the DSI aware test fixture
 */
trait ShutdownLifeCycle {
  private[this] var shutdownActions: List[() => Unit] = Nil
  private[this] val useCount = new java.util.concurrent.atomic.AtomicInteger(0)
  private[this] val log = getLogger[ShutdownLifeCycle]

  /**
   * Adds a shutdown action, which is called in reverse order on shutdown.
   */
  def addShutdownAction(action: => Unit): Unit = useCount.synchronized {
    shutdownActions = { () =>
      action
    } :: shutdownActions
  }

  /**
   * Call to wrap a block of code you want run that uses the object with the `ShutdownLifeCycle`, so it isn't
   * automatically shutdown until after the block completes.
   *
   * On return, if no one else is using this instance, then calls `shutdown()`.
   */
  def doUsing[A](block: => A): A =
    try {
      useCount.incrementAndGet()
      block
    } finally {
      if (useCount.decrementAndGet() == 0) shutdown()
    }

  /**
   * Runs the shutdown actions, in reverse order to that in which they were added. Visibility is optimus as used from
   * optimus.platform and optimus.dist
   */
  protected[optimus] def shutdown(): Unit = {
    log.trace("Shutdown: started")
    val problems = for {
      op <- shutdownActions
      e <- Try(op()).failed.toOption
    } yield e match {
      case NonFatal(e) => e
      case _           => throw e
    }
    if (problems.nonEmpty) {
      val error = new RuntimeException("Unexpected exception(s) during runtime shutdown")
      problems.foreach(error.addSuppressed(_))
      throw error
    }
  }

  /**
   * Forces shutdown, even if there are current ''users''.
   *
   * You can call this to force shutdown even if `doUsing()` was never called.
   */
  protected[optimus /*platform*/ ] def dispose(): Unit = shutdown()

}
