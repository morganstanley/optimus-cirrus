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
package optimus.graph

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.core.ArrayListWithMarker
import optimus.exceptions.RTExceptionTrait
import optimus.graph.GraphException.pathAsString
import optimus.graph.cache.NCSupport
import optimus.graph.diagnostics.NodeName
import optimus.graph.tracking.EventCause
import optimus.platform.EvaluationContext
import optimus.platform.util.PrettyStringBuilder

import scala.util.{Try, Failure, Success}
import java.util.{ArrayList => JArrayList}

object GraphException {

  def tryReallyHard(f: => String)(e: Throwable => String): String = {
    Try(f) match {
      case Success(s) => s
      case Failure(ex) =>
        Try(e(ex)).getOrElse(s"Exception generating exception message for ${ex.getClass.getName}")
    }
  }

  def pathAsString(): String = {
    EvaluationContext.currentNode.enqueuerChain()
    tryReallyHard {
      if (DiagnosticSettings.awaitStacks)
        EvaluationContext.currentNode.enqueuerChain()
      else
        GraphException.pathAsString(EvaluationContext.currentNode.nodeStackAny)
    } { e =>
      s"Exception generating node stack: ${e.getMessage}"
    }
  }

  def scenarioStackString(): String =
    tryReallyHard(EvaluationContext.currentNode.scenarioStack().prettyString)(e =>
      s"Exception generating scenario stack: ${e.getMessage}")

  def pathAsString(path: JArrayList[NodeTask], includeArgs: Boolean = false): String = {
    if (path eq null) "[no path]"
    else {
      val sb = new PrettyStringBuilder()
      sb.useEntityType = true
      val detailed = Settings.detailedCircularReferenceExceptions
      sb.showEntityToString = detailed
      sb.showNodeArgs = detailed
      sb.appendln("Node Stack:")
      path.forEach { n =>
        if (n eq null) // surprising if a null entry made it into the list, this is probably a paranoid check...
          sb.append("[invalid entry]")
        else if (DiagnosticSettings.proxyInWaitChain || !n.executionInfo().isProxy) {
          n.writePrettyString(sb)
          sb.append(" at ")
          sb.appendln(n.nameAndSource())
        }
      }
      sb.toString
    }
  }

  def verifyOffGraphException(path: JArrayList[NodeTask]): GraphFailedTrustException = {
    val msg = pathAsString(path)
    new GraphFailedTrustException(msg)
  }

}

/**
 * Base class for all (most?) exceptions thrown by graph runtime
 */
class GraphException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this() = this(null, null)
  def this(message: String) = this(message, null)
}

/** A graph runtime exception which is RT */
class RTGraphException(message: String, cause: Throwable) extends GraphException(message, cause) with RTExceptionTrait {
  def this() = this(null, null)
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
}

private[graph] object MandatedExceptionLogger {
  val log: Logger = getLogger(this.getClass)
}

class GraphFailedTrustException private[graph] (nodes: String) extends GraphException(nodes) {
  override def getMessage: String = s"Can't call non RT function from nodes: $nodes"

  MandatedExceptionLogger.log.error("GraphFailedTrustException constructed - logging always", this)
}

class GraphInInvalidState(message: String, cause: Throwable) extends GraphException(message, cause) {
  def this(message: String) = this(message, null)
  def this() = this("Graph is in invalid state", null)

  MandatedExceptionLogger.log.error("GraphInInvalidState constructed - logging always", this)
}

class EventCauseInInvalidState(message: String, eventCause: EventCause) extends GraphException(message) {

  MandatedExceptionLogger.log.error("EventCauseInInvalidState constructed - logging always", this)
}

class NodeCancelledException(node: NodeTask) extends GraphException {
  override def getMessage: String = "Execution of node " + node.toPrettyName(true, false) + " has been cancelled"
}

/**
 * Thrown when Circular Reference/Dependence is found while evaluating a graph node
 */
class CircularReferenceException(val fullCycle: ArrayListWithMarker[NodeTask]) extends GraphException {
  final def cycle: JArrayList[NodeTask] =
    if (DiagnosticSettings.proxyInWaitChain) fullCycle else NCSupport.removeProxies(fullCycle)
  // we include the arguments in the message since these are often very useful for identifying the cause of the cycle
  override def getMessage: String = pathAsString(cycle, includeArgs = true)
}

/**
 * Currently thrown when graph runtime detects that some user promises have been violated. e.g. Node marked as
 * \@scenarioIndependent directly (should be caught at compile time) or indirectly calls
 */
class IllegalScenarioDependenceException(val nonSInode: NodeTask, val path: JArrayList[NodeTask])
    extends GraphException
    with RTExceptionTrait {

  def this(key: NodeKey[_], requestingNode: NodeTask) =
    this(key.asInstanceOf[NodeTask], if (requestingNode eq null) null else requestingNode.nodeStackToSI)

  override def getMessage: String = {
    "Cannot invoke non-@scenarioIndependent node " + nonSInode.toPrettyName(
      true,
      false) + " from @scenarioIndependent node\n" + pathAsString(path)
  }
}

// thrown when non-SI auditor reads tweakables when auditing SI node
class IllegalScenarioDependenceInAuditor(
    override val nonSInode: NodeTask,
    val auditorNode: NodeTask,
    val auditedNode: NodeTask)
    extends IllegalScenarioDependenceException(nonSInode, path = auditedNode.nodeStackToSI) {
  override def getMessage: String = {
    "Cannot invoke non-@scenarioIndependent node " + nonSInode.toPrettyName(true, false) +
      " when auditing " + auditedNode.toSimpleName + " at\n" + pathAsString(auditorNode.nodeStackToNode)
  }
}

/**
 * Thrown when a givenOverlay or evaluateIn call is detected within a scenario independent scenario stack.
 */
class IllegalEvaluationInOtherScenarioInSIException(cause: Throwable = null) extends RTGraphException(cause) {
  override def getMessage = "Cannot use givenOverlay or evaluateIn within a scenario independent stack"
}

/**
 * Thrown when a givenOverlay call is detected within the stack of a cross scenario node.
 */
class IllegalEvaluationInOtherScenarioInXSException(xsNode: NodeName) extends RTGraphException {
  override def getMessage = s"Cannot use givenOverlay or evaluateIn within cross scenario node $xsNode"
}

/**
 * Base trait for exceptions thrown as part of normal flow control
 */
trait FlowControlException
