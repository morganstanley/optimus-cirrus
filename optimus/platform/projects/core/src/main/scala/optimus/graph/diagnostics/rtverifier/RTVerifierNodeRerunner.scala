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
package optimus.graph.diagnostics.rtverifier

import optimus.debug.InstrumentedEquals
import optimus.debug.RTVerifierCategory
import optimus.graph.DiagnosticSettings
import optimus.graph.NodeAwaiter
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.platform.EvaluationQueue

object RTVerifierNodeRerunner {

  /**
   * Reruns a node after its completion to ensure it is RT by enqueuing and waiting on its clone.
   */
  class Rerunner(original: NodeTask) extends NodeAwaiter {
    // we are reusing the remote flag to mark cloned nodes,
    // let's make sure we are not comparing remote nodes
    // we can revisit this if needed, but we would need to add a dedicated flag for it.
    require(!original.isRemoted, "Comparing remote nodes is not supported")

    override def onChildCompleted(eq: EvaluationQueue, ntsk: NodeTask): Unit = {
      // if ntsk is an original node (isRemoted = false), then we create a clone, enqueue and wait on it
      if (!ntsk.isRemoted) {
        val clonedNtsk = ntsk.DEBUG_ONLY_cloneTask()
        // using flag STATE_REMOTED to mark cloned nodes so that we do not clone them again
        clonedNtsk.markRemoted()
        eq.enqueue(clonedNtsk)
        clonedNtsk.continueWith(this, eq)
      }
      // if ntsk is a clone (isRemoted = true), then both original and clone are completed
      // and we are ready to compare their results
      else {
        val originalResult = resultOrException(original)
        val clonedResult = resultOrException(ntsk)
        if (!equalsEnough(originalResult, clonedResult)) {
          val key = original.executionInfo().fullName()
          val details =
            s"""Detected non-RT node $key.
               |On rerun the node returned two different results:
               |$originalResult
               |...which is different from...
               |$clonedResult
               |Please use the graph debugger for more information.""".stripMargin
          RTVerifierReporter.reportClassViolation(
            category = RTVerifierCategory.NON_RT_NODE,
            key = key,
            details = details,
            clazzName = original.getClass.getName)
        }
      }
    }

    private def resultOrException(ntsk: NodeTask): AnyRef =
      if (ntsk.isDoneWithException) ntsk.exception() else ntsk.resultObjectEvenIfIncomplete()

    private def equalsEnough(objA: AnyRef, objB: AnyRef): Boolean = (objA, objB) match {
      // unfortunately exceptions do not have a stable equality,
      // the reason why we need to relax what we mean with "equals" here
      case (a: Throwable, b: Throwable) => // regex-ignore-line: false alarm, we are not catching exceptions here!
        a.getClass == b.getClass
      case (a, b) => equalityCheck(a, b, skipBadEquality = DiagnosticSettings.rtvNodeRerunnerSkipBadEquality)
    }

    private def equalityCheck(objA: AnyRef, objB: AnyRef, skipBadEquality: Boolean): Boolean = {
      if (skipBadEquality) InstrumentedEquals.enterReporting()
      val areEqual = objA.equals(objB)
      if (skipBadEquality) InstrumentedEquals.exitReporting()
      def isBadEquality = skipBadEquality && InstrumentedEquals.isBadEquality
      areEqual || isBadEquality
    }
  }

  def testRTness(ntsk: NodeTask, ec: OGSchedulerContext): Unit = {
    // do not verify that a NodeTask is RT if any of the following is true:
    // - it's manually excluded
    // - it's already a clone (e.g., is flagged as remote)
    // - it's actually not a node (e.g., internal + user-attributable)
    // - it cannot be cloned (necessary for the rerun)
    val ntskInfo = ntsk.executionInfo()
    if (ntskInfo.isExcludedFromRTVNodeRerunner || ntsk.isRemoted || ntskInfo.isNotNode || !ntsk.isClonable) return

    ntsk.continueWith(new Rerunner(ntsk), ec)
  }

}
