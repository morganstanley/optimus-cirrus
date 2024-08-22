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
package optimus.profiler.recipes

import optimus.debugger.browser.ui.GraphBrowserAPI
import optimus.graph.NodeTask
import optimus.graph.NodeTrace
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.profiler.recipes.TreeComparisonResult.PNodeTaskPair
import optimus.profiler.recipes.TreeComparisonResult.noDiffInArgs
import optimus.profiler.ui.Fonts

import scala.collection.mutable.ArrayBuffer

object TreeComparisonResult {
  def LOOK(text: String): String = "<span class=look>" + text + "</span>"
  def NOTE(text: String): String = "<div class=notes>" + text + "</div><br>"
  def DEF(task: PNodeTask)(body: String): String = {
    "<div class=code><span class=def>def</span> " + task
      .nameAndModifier() + "(...) { <br><div class=blk>" + body + "</div>}</div>"
  }

  val cmdSkipComparingArgsInNode = "cmdSkipComparingArgsInNode"
  val cmdSkipComparingNode = "cmdSkipComparingNode"
  def SkipComparingArgsInNode(task: PNodeTask): String = {
    s"<a href=$cmdSkipComparingArgsInNode>skip comparing arguments</a> to <b>this specific call</b> to ${task.nameAndModifier()}"
  }
  def SkipComparingNode(task: PNodeTask): String = {
    s"<a href=$cmdSkipComparingArgsInNode>skip comparing this specific ${task.nameAndModifier()} node</a>"
  }

  private val startStyle = "<style type='text/css'>" +
    s"body { font-family: Arial; font-size: ${Math.round(12 * Fonts.multiplier)}pt; } " +
    ".def { font-weight: bold } " +
    ".code { font-family: Consolas; margin: 10; }" +
    ".blk { margin-left: 10; } " +
    ".look { font-style: italic; color: #ff0000; } " +
    ".notes { margin-left: 20; margin-right: 20; padding:10; background-color: #fdfdf0; } " +
    "</style>"

  def asHtml(tcr: TreeComparisonResult)(body: String): String = {
    "<html>" + startStyle + "<body>" + body +
      s"<br><br><br>More commands are available via right click on the nodes<br> " +
      "</body></html>"
  }

  type PNodeTaskPair = (PNodeTask, PNodeTask)
  val empty = TreeComparisonResult(DiffReason.None, Array.empty[PNodeTaskPair])
  val noDiffInArgs = DiffReason.Args(Array.empty[DifferentArgs])
}

final case class TreeComparisonResult(reason: DiffReason, path: Array[PNodeTaskPair]) {
  def branchA: PNodeTask = path.head._1
  def branchB: PNodeTask = path.head._2
  def childA: PNodeTask = path.last._1
  def childB: PNodeTask = path.last._2
  def callerA: PNodeTask = path(path.length - 2)._1
}

final case class DifferentArgs(argA: Any, argB: Any)

sealed trait DiffReason {
  def compareA: Any = null // for Debugger inspect tab
  def compareB: Any = null
  def description: String = getClass.getSimpleName.stripSuffix("$")
  def htmlDescription(tcr: TreeComparisonResult): String
}

object DiffReason {
  import TreeComparisonResult._
  case object None extends DiffReason {
    override def description: String = ""
    override def htmlDescription(tcr: TreeComparisonResult): String = asHtml(tcr)("Nodes match!")
  }

  case object LocalCompute extends DiffReason {
    override def htmlDescription(tcr: TreeComparisonResult): String = asHtml(tcr) {
      DEF(tcr.childA) {
        LOOK("// computed different result <br>")
      } + NOTE("Inspect the nodes using the 'diff view' on the right")
    }
  }

  final case class Args(differentArgs: Array[DifferentArgs]) extends DiffReason {
    def nonEmpty: Boolean = differentArgs.nonEmpty
    override def compareA: Any = differentArgs.map(_.argA)
    override def compareB: Any = differentArgs.map(_.argB) // one in each inspect tab?
    override def htmlDescription(tcr: TreeComparisonResult): String = asHtml(tcr) {
      DEF(tcr.callerA) {
        LOOK("// prepared different argument(s) to a call to<br>") +
          s"${tcr.childA.nameAndModifier}(" + LOOK("args") + ")"
      } +
        NOTE("Inspect the arguments using the 'diff view' on the right") +
        NOTE(
          "Sometimes arguments are different even though underlying values are the same<br>" +
            "Adding proper hashCode/equals is required or often just marking <b>class</b> as <b>case class</b>" +
            " is enough<br>" +
            "Meanwhile you can " + SkipComparingArgsInNode(tcr.childA))
    }
  }

  final case class InstanceIdentity(arg: DifferentArgs) extends DiffReason {
    override def compareA: Any = arg.argA
    override def compareB: Any = arg.argB
    override def htmlDescription(tcr: TreeComparisonResult): String = asHtml(tcr) {
      DEF(tcr.callerA) {
        LOOK("// called on a different instance<br>") +
          s"${tcr.childA.nameAndModifier}(" + LOOK("this") + ")"
      } +
        NOTE("Inspect the arguments using the 'diff view' on the right") +
        NOTE(
          "Sometimes arguments are different even though underlying values are the same<br>" +
            "Adding proper hashCode/equals is required or often just marking <b>class</b> as <b>case class</b>" +
            " is enough<br>" +
            "Meanwhile you can " + SkipComparingArgsInNode(tcr.childA))
    }
  }

  final case class ChildName(branchA: NodeName, branchB: NodeName) extends DiffReason {
    override def compareA: Any = branchA
    override def compareB: Any = branchB
    override def htmlDescription(tcr: TreeComparisonResult): String = asHtml(tcr) {
      DEF(tcr.callerA) {
        LOOK("// Called in one case: <br>") +
          s"${tcr.childA.nameAndModifier}(args)<br>" +
          LOOK("// Called in the other case: <br>") +
          s"${tcr.childB.nameAndModifier}(args)<br>"
      } +
        NOTE("Inspect the nodes using the 'diff view' on the right") +
        NOTE(
          "Sometimes node was just renamed, or it was tweaked to the same value.<br>" +
            "You can " + SkipComparingNode(tcr.childA))
    }
  }

  final case class ChildCount(branchA: Int, branchB: Int) extends DiffReason {
    override def compareA: Any = branchA
    override def compareB: Any = branchB
    override def htmlDescription(tcr: TreeComparisonResult): String = asHtml(tcr) {
      DEF(tcr.callerA) {
        (if (tcr.childA ne null) LOOK("// Called in one case: <br>") + s"${tcr.childA.nameAndModifier}(args)<br>"
         else LOOK("// Called nothing in the first case... <br>")) +
          (if (tcr.childB ne null) LOOK("// Called in the other case:") + s"${tcr.childB.nameAndModifier}(args)<br>"
           else LOOK("// Called nothing in the other case...<br>"))
      } +
        NOTE("Inspect the node using the 'diff view' on the right") +
        NOTE(
          s"This usually indicates the difference came from a the ${tcr.callerA.nodeName().name}.<br>" +
            "You can " + SkipComparingNode(tcr.callerA))
    }
  }

  /** Special case if the inputs should not be even compared */
  final case class OriginalInputs(underlyingReason: DiffReason) extends DiffReason {
    override def compareA: Any = underlyingReason.compareA
    override def compareB: Any = underlyingReason.compareB
    override def htmlDescription(tcr: TreeComparisonResult): String = asHtml(tcr) {
      "Original inputs are different and there is no point in continuing this comparison."
    }
  }
}

object CompareNodeDiffs {
  def compareTrees(a: PNodeTask, b: PNodeTask, structureOnly: Boolean = false): TreeComparisonResult = {
    val inputDiff = compareInputs(a, b)
    if (inputDiff ne DiffReason.None)
      TreeComparisonResult(DiffReason.OriginalInputs(inputDiff), Array((a, b)))
    else {
      val v = new Visitor(structureOnly)
      try v.findFirstDifference(a, b)
      finally v.resetVisited()
      v.result
    }
  }

  // never rerun a proxy node, always the underlying (most users won't be looking at proxies but just in case they do...)
  def reevaluateAndCompareLive(task: NodeTask): TreeComparisonResult =
    reevaluateAndCompareLive(NodeTrace.accessProfile(task.cacheUnderlyingNode))

  def reevaluateAndCompareLive(task: PNodeTask): TreeComparisonResult = {
    if (!task.isLive)
      throw new IllegalArgumentException("Reevaluate only works for live tracing")
    val ntsk = task.getTask
    // never rerun a proxy node. note: GraphBrowserAPI.invalidateAndRecompute handles this already but we want the
    // 'original' task to rerun to actually be the underlying node too, otherwise our 'diff' is just in branches,
    // since the proxy depends on the underlying node
    val toRerun = ntsk.cacheUnderlyingNode
    val reevaluated = GraphBrowserAPI.invalidateAndRecompute(toRerun, clearCache = true)
    val original = if (toRerun eq ntsk) task else NodeTrace.accessProfile(toRerun)
    compareTrees(original, reevaluated)
  }

  private def compareIdentity(childA: PNodeTask, childB: PNodeTask): DiffReason = {
    val instanceA = childA.methodThisKey()
    val instanceB = childB.methodThisKey()
    if (instanceA != instanceB)
      DiffReason.InstanceIdentity(DifferentArgs(instanceA, instanceB))
    else
      DiffReason.None
  }

  /** don't bail early, just collect all different args */
  private def compareArgs(childA: PNodeTask, childB: PNodeTask): DiffReason.Args = {
    val argsA = childA.args
    val argsB = childB.args
    if ((argsA ne null) && (argsB ne null)) {
      val differentArgs = ArrayBuffer[DifferentArgs]()
      var i = 0
      val maxLength = Math.max(argsA.length, argsB.length)
      while (i < maxLength) {
        if (i < argsA.length) {
          val argA = argsA(i)
          if (i < argsB.length) {
            val argB = argsB(i)
            if (argA != argB)
              differentArgs += DifferentArgs(argA, argB)
          } else
            differentArgs += DifferentArgs(argA, null) // arg exists in A and not in B
        } else {
          val argB = argsB(i)
          differentArgs += DifferentArgs(null, argB) // arg exists in B and not in A
        }
        i += 1
      }
      if (differentArgs.nonEmpty) DiffReason.Args(differentArgs.toArray) else noDiffInArgs
    } else {
      if (argsA ne null) DiffReason.Args(argsA.map(DifferentArgs(_, null))) // all Bs are null
      else if (argsB ne null) DiffReason.Args(argsB.map(DifferentArgs(_, null))) // all As are null
      else noDiffInArgs // both null, ie no diff
    }
  }

  private def compareNames(childA: PNodeTask, childB: PNodeTask): DiffReason = {
    val nameA = childA.nodeName
    val nameB = childB.nodeName
    if (nameA != nameB)
      DiffReason.ChildName(nameA, nameB)
    else
      DiffReason.None
  }

  /** Compares names of the tasks and entity, scenario */
  private def compareInputs(childA: PNodeTask, childB: PNodeTask): DiffReason = {
    val nameA = childA.nodeName
    val nameB = childB.nodeName

    if (nameA != nameB)
      DiffReason.ChildName(nameA, nameB)
    else if (
      childA.ignoreArgumentsInComparison || childB.ignoreArgumentsInComparison || childA.getTask
        .executionInfo()
        .isIgnoredInProfiler
    )
      DiffReason.None
    else {
      val identityMatch = compareIdentity(childA, childB)
      if (identityMatch != DiffReason.None)
        identityMatch
      else {
        val mismatchedArgs = compareArgs(childA, childB)
        if (mismatchedArgs.nonEmpty)
          mismatchedArgs
        else
          DiffReason.None
      }
    }
  }

  private final class Visitor(structureOnly: Boolean) {
    import optimus.profiler.extensions.PNodeTaskExtension._

    private[recipes] var result: TreeComparisonResult = TreeComparisonResult.empty
    private val path = ArrayBuffer[PNodeTaskPair]()

    /** branch counts */
    private var countA = 0
    private var countB = 0

    def findFirstDifference(a: PNodeTask, b: PNodeTask): Boolean = {
      if (a.getTask.executionInfo.isIgnoredInProfiler || a.ignoreResultInComparison || a.resultKey == b.resultKey)
        false
      else {
        // The current pair (a, b) has different results and this could be because of the values
        // of children and if not, this current (a, b) needs to be reported
        path += ((a, b))
        val itA = a.getCalleesWithoutInternal
        val itB = b.getCalleesWithoutInternal // these should be the same! handle case where different
        while (itA.hasNext && itB.hasNext) {
          val childA = itA.next()
          val childB = itB.next()
          countA += 1
          countB += 1
          val inputsDiffReason = if (structureOnly) compareNames(childA, childB) else compareInputs(childA, childB)
          if (DiffReason.None ne inputsDiffReason) {
            path += ((childA, childB)) // won't recurse so need to add children here
            result = TreeComparisonResult(inputsDiffReason, path.toArray)
            return true
          }
          if (findFirstDifference(childA, childB))
            return true
        }

        if (itA.hasNext || itB.hasNext) {
          val childA = if (itA.hasNext) { countA += 1; itA.next() }
          else null
          val childB = if (itB.hasNext) { countB += 1; itB.next() }
          else null
          path += ((childA, childB)) // might be adding nulls here, deal with them in ui
          result = TreeComparisonResult(DiffReason.ChildCount(countA, countB), path.toArray)
          return true
        }

        if (structureOnly)
          false // There are real differences but it's not structureOnly
        else {
          // All children were checked, the blame is with the current pair of nodes, which were added to path already
          result = TreeComparisonResult(DiffReason.LocalCompute, path.toArray)
          true
        }
      }
    }

    /** If we ever use visited index in PNT in this traversal, come back to this and reset it here! */
    def resetVisited(): Unit = {
      // reset branch counts and path since we'll never refer to them externally
      countA = 0
      countB = 0
      path.clear()
    }
  }
}
