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

import msjava.slf4jutils.scalalog._
import optimus.platform.debugger.StackElemData
import optimus.platform.debugger.StackElemType
import optimus.graph.diagnostics.NodeName
import optimus.platform.EvaluationContext
import optimus.platform.StartNode
import optimus.scalacompat.collection._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * This class includes helper methods for parsing JVM stack traces and synthesising them with wait chains in order to
 * create full Optimus stacks
 *
 * Input a JVM stack of the form,
 *
 * case 1 (SyncStack):
 *
 * frame1 frame2 frame3 o.g.OGSchedulerContext.runAndWait (MARKER) ...
 *
 * or
 *
 * case 2 (Worker thread)
 *
 * frame1 frame2 frame3 o.g.OGScheduer$OGThread.run (MARKER)
 *
 *   1. Iterate over the stack elements, splitting the list at the point where we find a MARKER
 *   1. Drop the top sub-list (i.e. part of the stack up to the first MARKER - displayed by default in the debugger).
 *      For the remaining sub-lists (each of which represents a node call):
 *   1. Look at the waiters for that node:
 *      1. If it is a sync frame, then search for the thread that has the stack that called into this sync-frame (which
 *         must exist because they are synchronously waiting) and append it to the current stack
 *      1. If it is a NodeTask, then append that node call onto the current stack as if it were a normal stack element
 *
 * The result is a stack that might look something like the following:
 *
 * frame1 (thread a) frame2 (thread b) nodeCall1 nodeCall2 frame1 (thread b) nodeCall3 frame1 (thread c) frame2 (thread
 * c) o.g.OGScheduler$OGThread.run
 *
 * Once we have the stacks, we then convert them into a CSV representation (detailed below) that can be parsed by the
 * IntelliJ plugin.
 */
object NodeStacks {

  val log: Logger = getLogger(getClass)

  // This def is used as a placeholder for node tasks that don't have a clear class / method name to reference
  def noStackInfo(): Unit = throw new GraphException("Should never be called directly")

  // Is the frame (1) a marker for a sync call into the graph, or (2) the first frame on an OGThread
  def isSyncMarker(frame: StackTraceElement): Boolean = {
    val className = frame.getClassName
    val methodName = frame.getMethodName
    // It would be nice to be able to split on OGSchedulerContext.run since this is common to all on-graph stack
    // traces but this doesn't work in the case of a worker thread thread stealing work - in that case, we
    // would end up missing a split
    (className == "optimus.graph.OGSchedulerContext" && methodName == "runAndWait") ||
    (className == "optimus.graph.OGScheduler$OGThread" && methodName == "run")
  }

  def jvmStacksSplitBySyncFrame: collection.Map[Thread, collection.IndexedSeq[collection.Seq[StackTraceElement]]] =
    Thread.getAllStackTraces.asScala.mapValuesNow(splitAtSyncFrame[StackTraceElement](_, isSyncMarker)).toMap

  @tailrec def splitAtSyncFrame[T](
      in: collection.Seq[T],
      isSyncMarker: T => Boolean,
      out: ArrayBuffer[collection.Seq[T]] = ArrayBuffer.empty[collection.Seq[T]]
  ): collection.IndexedSeq[collection.Seq[T]] = {
    in match {
      // We append to the end of the buffer for efficiency and so must reverse it once we are done
      case Nil => out.reverse
      // Need the extra empty frame so that sync frame indices and the output array indices match up
      case collection.Seq(x) if isSyncMarker(x) =>
        splitAtSyncFrame(collection.Seq.empty, isSyncMarker, out += collection.Seq.empty)
      // But we don't want the empty frame if we're not at the end of the stack
      case x +: xs if isSyncMarker(x) => splitAtSyncFrame(xs, isSyncMarker, out)
      case _                          =>
        // If sync frame is not first elem, then size of in is guaranteed to decrease
        splitAtSyncFrame(in.dropWhile(!isSyncMarker(_)), isSyncMarker, out += in.takeWhile(!isSyncMarker(_)))
    }
  }

  // Useful method for debugging in case of parsing issues
  def debugJvmStacks(): Unit =
    NodeStacks.jvmStacksSplitBySyncFrame.foreach { case (thread, stacks) =>
      log.info(thread.getName)
      var splitIdx = 0
      stacks.foreach(stack => {
        log.info(s"Stacks for split: $splitIdx")
        stack.foreach(elem => log.info(elem.toString))
        splitIdx += 1
      })
    }

  private def getStackElemData(stackElem: StackElem): StackElemData = {
    stackElem match {
      case NodeStackElem(ntsk) =>
        try {
          val lineNumber = ntsk.stackTraceElem.getLineNumber
          val (className, methodRawName, methodDisplayName) = {
            val cls = ntsk.getClass
            val execInfoName = ntsk.executionInfo().name()
            ntsk match {
              case p: PropertyNode[_] =>
                val owningCls = if (p.entity ne null) p.entity.$info.runtimeClass else cls
                (owningCls.getName, execInfoName, p.propertyInfo.name())
              case _ =>
                // Basically presume that this is manually written node and so get the class and use the run method on it
                val cleanName = NodeName.cleanNodeClassName(cls)
                val methodName = cleanName.substring(cleanName.lastIndexOf('.') + 1)
                (cls.getName, "run", s"$execInfoName $methodName")
            }
          }
          StackElemData(className, methodRawName, s"(node) $methodDisplayName", lineNumber, StackElemType.node)
        } catch {
          case ex: Exception =>
            log.error(s"Unable to extract stack trace information for frame - skipping", ex)
            StackElemData("unable to extract frame (see log for error)", "UNKNOWN", "UNKNOWN", 1, StackElemType.node)
        }
      case JvmStackElem(elem) =>
        StackElemData(elem.getClassName, elem.getMethodName, elem.getMethodName, elem.getLineNumber, StackElemType.jvm)
    }

  }

  def reconstitutedNodeAndJvmStack(nodeTask: NodeTask): Seq[StackElem] = {
    val syncStacks = jvmStacksSplitBySyncFrame
    nodeTask
      .dbgWaitChain(false)
      .asScala
      .collect {
        case _: StartNode   => Nil
        case task: NodeTask => NodeStackElem(task.cacheUnderlyingNode) :: Nil
        case sync: OGSchedulerContext#SyncFrame =>
          val stack = syncStacks.get(sync.thread)
          if (stack.isEmpty) Nil
          else stack.get(sync.index).map(JvmStackElem)
      }
      .flatten
  }

  def reconstitutedNodeAndJvmStack(): Array[StackElem] =
    reconstitutedNodeAndJvmStack(EvaluationContext.currentNode).toArray

  def reconstitutedNodeAndJvmStackAsStrings(nodeTask: NodeTask): Seq[String] =
    reconstitutedNodeAndJvmStack(nodeTask).map(getStackElemData).map(StackElemData.write)

  def reconstitutedNodeAndJvmStackAsStrings(): Array[String] =
    reconstitutedNodeAndJvmStackAsStrings(EvaluationContext.currentNode).toArray
}

sealed trait StackElem
final case class NodeStackElem(ntsk: NodeTask) extends StackElem
final case class JvmStackElem(elem: StackTraceElement) extends StackElem
