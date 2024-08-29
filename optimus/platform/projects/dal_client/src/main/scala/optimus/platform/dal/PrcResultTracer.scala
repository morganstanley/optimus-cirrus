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
package optimus.platform.dal

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.dsi.bitemporal.DalPrcRedirectionResult
import optimus.platform.internal.SimpleGlobalStateHolder
import org.kohsuke.args4j

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class PrcResultTracerException(msg: String, cause: Throwable = None.orNull) extends Exception(msg, cause)

trait PrcResultTracerCmdLine {
  @args4j.Option(
    name = "-rc",
    aliases = Array("--redirection-count"),
    required = false,
    usage = "at most redirection count from Prc"
  )
  val maxRedirectionCountFromPrc = 0

  @args4j.Option(
    name = "-ec",
    aliases = Array("--execution-count"),
    required = false,
    usage = "at least execution count by Prc"
  )
  val minExecutionCountByPrc = 1

  def validate(): Unit = {
    require(maxRedirectionCountFromPrc >= 0, "redirection count may not be negative")
    require(minExecutionCountByPrc >= 0, "prc execution count may not be negative")
  }
}

trait PrcResultTracer {
  def onRedirection(redirectionResult: DalPrcRedirectionResult): Unit
  def onPrcExecution(cmdCount: Int): Unit
  def reset(): Unit
}

object NoopResultTracer extends PrcResultTracer {
  import PrcResultTracerState._

  override def onRedirection(redirectionResult: DalPrcRedirectionResult): Unit = {
    log.debug(s"Redirect command to broker because of reason: ${redirectionResult.redirectionReason}")
  }

  override def reset(): Unit = {}
  override def onPrcExecution(cmdCount: Int): Unit = {
    log.debug(s"Prc execution detected for $cmdCount cmds")
  }
}

class SimpleCountingResultTracerWithRedirectionDetails extends SimpleCountingResultTracer {
  import PrcResultTracerState._
  private val redirectionDetails = new ListBuffer[DalPrcRedirectionResult]()

  override def onRedirection(redirectionResult: DalPrcRedirectionResult): Unit = {
    synchronized {
      redirectionDetails.+=(redirectionResult)
    }
    super.onRedirection(redirectionResult)
  }

  override def ensureAtMostNRedirections(redirectionCount: Int, atMost: Int, msg: String): Unit = {
    val msgWithRedirections = synchronized {
      s"$msg RedirectionDetails: ${redirectionDetails.mkString(";")}"
    }
    super.ensureAtMostNRedirections(redirectionCount, atMost, msgWithRedirections)
  }

  override def reset(): Unit = {
    synchronized {
      redirectionDetails.clear()
    }
    super.reset()
  }
}
class SimpleCountingResultTracer extends PrcResultTracer {
  import PrcResultTracerState._

  private val prcExecutionCounter = new AtomicInteger(0)
  private val redirectionsSeen = new LinkedBlockingQueue[DalPrcRedirectionResult]()

  override def onRedirection(redirectionResult: DalPrcRedirectionResult): Unit = {
    redirectionsSeen.put(redirectionResult)
    ()
  }

  override def onPrcExecution(cmdCount: Int): Unit = {
    prcExecutionCounter.addAndGet(cmdCount)
  }

  override def reset(): Unit = {
    redirectionsSeen.clear()
    prcExecutionCounter.set(0)
  }

  private[optimus /*platform*/ ] def getRedirections: Seq[DalPrcRedirectionResult] = {
    val drain = new java.util.ArrayList[DalPrcRedirectionResult]()
    redirectionsSeen.drainTo(drain)
    drain.asScala
  }

  private[optimus /*platform*/ ] def getPrcExecutionCounter: Int = prcExecutionCounter.get()

  protected def ensureAtMostNRedirections(redirectionCount: Int, atMost: Int, msg: String): Unit = {
    log.info(s"${redirectionMsg(redirectionCount)} msg: $msg")
    if (redirectionCount > atMost) {
      throw new PrcResultTracerException(
        s"Expected at most $atMost redirection(s), got $redirectionCount redirection(s) $msg")
    }
  }

  private def ensureAtLeastNPrcExecutions(executionCount: Int, atLeast: Int, msg: String): Unit = {
    log.info(executionMsg(executionCount))
    if (executionCount < atLeast) {
      throw new PrcResultTracerException(
        s"Expected at least $atLeast Prc execution(s), got $executionCount execution(s) $msg")
    }
  }

  def validateTraces(atMostRedirections: Int, atLeastExecutions: Int, msg: String = ""): Unit = {
    ensureAtMostNRedirections(getRedirections.size, atMostRedirections, msg)
    ensureAtLeastNPrcExecutions(getPrcExecutionCounter, atLeastExecutions, msg)
  }
}

class PrcResultTracerState {
  private var currPrcResultTracer: PrcResultTracer = PrcResultTracerState.defaultPrcRequestTracer
}

object PrcResultTracerState extends SimpleGlobalStateHolder(() => new PrcResultTracerState) {
  val log = getLogger[PrcResultTracer]

  private val defaultPrcRequestTracer = NoopResultTracer

  def setPrcResultTracer(prcResultTracer: PrcResultTracer): Unit = synchronized {
    getState.currPrcResultTracer = prcResultTracer
  }

  def getPrcResultTracer: PrcResultTracer = synchronized {
    getState.currPrcResultTracer
  }

  def resetPrcResultTracer(): Unit = synchronized {
    getState.currPrcResultTracer.reset()
    setPrcResultTracer(defaultPrcRequestTracer)
  }

  def redirectionMsg(count: Int): String = s"PRC redirection(s) detected: $count"

  def executionMsg(count: Int): String = s"PRC execution(s) detected: $count"

}
