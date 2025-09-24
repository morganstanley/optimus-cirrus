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
import optimus.graph.tracking.EventCause
import optimus.platform.StartNode
import optimus.scalacompat.collection._

import scala.annotation.tailrec
import scala.collection.immutable.ArraySeq
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
 * frame1 frame2 frame3 o.g.OGScheduler$OGThread.run (MARKER)
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
 * Once we have the stacks, we convert them into jsons that intellij can parse.
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

  private def buildSyncStack(
      syncStacksForThread: IndexedSeq[IndexedSeq[StackTraceElement]],
      frame: OGSchedulerContext#SyncFrame): List[StackElem] = {
    if (DiagnosticSettings.collapseSyncStacksInDebugger) {
      val ssFrames = syncStacksForThread(frame.index)
      val waitingOn = frame.getWaitingOn

      // For most sync stacks, we can find the guilty method that causes the sync stacks by grabbing the waitingOn
      // and finding the function that (likely) called it. This is a bit of a hack, but it works for normal property
      // nodes sync stacks.
      val likelySyncCall = Option(waitingOn)
        .map(_.classMethodName())
        .flatMap { toFind =>
          // we are looking for the frame right *after* the one that has the method name of the waiting on
          val i = ssFrames.indexWhere(s => toFind.endsWith(s.getMethodName))
          if ((i > -1) && (i + 1 < ssFrames.length))
            Some(ssFrames(i + 1))
          else None
        }
        // If we found the parent of the sync stack, we use it in the debugger display in the frame to make it nicer.
        // for @node foo -> bar1 -> bar2 -> @node baz, we end up with
        //
        // (node) baz
        // (sync stack) bar2
        // (node) foo
        //
        // with "bar1" folded in the sync stack frame, but findable. If we fail to find the our culprit we still have
        // all the frames folded away, we just show whatever is the top method as our "sync stack", which will probably
        // be something beautiful like "apply_22_33$fsm<init>" or whatever
        .getOrElse(ssFrames.head)

      CollapsedJvmStackElem(likelySyncCall, ssFrames.toArray) :: Nil
    } else syncStacksForThread(frame.index).map(JvmStackElem).toList
  }

  def jvmStacksSplitBySyncFrame: Map[Thread, IndexedSeq[IndexedSeq[StackTraceElement]]] =
    Thread.getAllStackTraces.asScala
      .mapValuesNow(t => splitAtSyncFrame[StackTraceElement](ArraySeq.unsafeWrapArray(t), isSyncMarker))
      .toMap

  def splitAtSyncFrame[T](
      in: ArraySeq[T],
      isSyncMarker: T => Boolean
  ): IndexedSeq[IndexedSeq[T]] = {
    @tailrec def splitRec(
        in: ArraySeq[T],
        isSyncMarker: T => Boolean,
        out: ArrayBuffer[ArraySeq[T]]
    ): IndexedSeq[IndexedSeq[T]] = {
      in match {
        // We append to the end of the buffer for efficiency and so must reverse it once we are done
        case Nil => out.view.reverse.to(ArraySeq)
        // Need the extra empty frame so that sync frame indices and the output array indices match up
        case Seq(x) if isSyncMarker(x) =>
          splitRec(ArraySeq.empty, isSyncMarker, out += ArraySeq.empty)
        // But we don't want the empty frame if we're not at the end of the stack
        case x +: xs if isSyncMarker(x) => splitRec(xs, isSyncMarker, out)
        case _                          =>
          // If sync frame is not first elem, then size of in is guaranteed to decrease
          splitRec(in.dropWhile(!isSyncMarker(_)), isSyncMarker, out += in.takeWhile(!isSyncMarker(_)))
      }
    }

    splitRec(in, isSyncMarker, ArrayBuffer.empty)
  }

  def reconstitutedNodeAndJvmStack(nodeTask: NodeTask): Array[StackElem] = {
    val syncStacks = jvmStacksSplitBySyncFrame
    nodeTask
      .dbgWaitChain(false)
      .asScala
      .iterator
      .collect {
        case _: StartNode   => Nil
        case task: NodeTask => NodeStackElem(task.cacheUnderlyingNode) :: Nil
        case sync: OGSchedulerContext#SyncFrame =>
          syncStacks.get(sync.thread) match {
            case Some(ss) => buildSyncStack(ss, sync)
            case None     => Nil
          }
        case ec: EventCause => List(ECStackElem(ec))
      }
      .flatten
      .to(Array)
  }
}

sealed trait StackElem {
  def toStackData: StackElemData
  def toStackString: String = StackElemData.write(toStackData)
}

final case class NodeStackElem(ntsk: NodeTask) extends StackElem {
  def toStackData: StackElemData = try {
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
      NodeStacks.log.error(s"Unable to extract stack trace information for frame - skipping", ex)
      StackElemData("unable to extract frame (see log for error)", "UNKNOWN", "UNKNOWN", 1, StackElemType.node)
  }
}

final case class JvmStackElem(elem: StackTraceElement) extends StackElem {
  def toStackData: StackElemData =
    StackElemData(elem.getClassName, elem.getMethodName, elem.getMethodName, elem.getLineNumber, StackElemType.jvm)
}

final case class ECStackElem(ec: EventCause) extends StackElem {
  def toStackData: StackElemData = StackElemData(ec.getClass.toString, "", ec.cause, -1, StackElemType.ec)
}

final case class CollapsedJvmStackElem(syncCall: StackTraceElement, val stack: Array[StackTraceElement])
    extends StackElem {
  override def toString: String = stack.map(_.toString).mkString("\n")
  def toStackData: StackElemData =
    StackElemData(
      syncCall.getClassName,
      syncCall.getMethodName,
      s"(sync stack) ${syncCall.getMethodName}",
      syncCall.getLineNumber,
      StackElemType.jvm)
}
