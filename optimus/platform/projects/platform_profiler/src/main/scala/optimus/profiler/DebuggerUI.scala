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
package optimus.profiler

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import java.awt.event.ActionListener
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.function.Predicate
import java.util.{ArrayList => JArrayList}
import java.util.{IdentityHashMap => JIdentityHashMap}
import optimus.graph._
import optimus.graph.cache.Caches
import optimus.graph.diagnostics.Debugger
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.messages.BookmarkCounter
import optimus.platform._
import optimus.platform.util.Log
import optimus.profiler.ui.browser.GraphBrowser
import optimus.profiler.ui.GraphConsole
import optimus.profiler.ui.GraphDebuggerUI
import optimus.profiler.ui.GroupConfigs
import optimus.profiler.ui.NodeProfiler
import optimus.profiler.ui.NodeStacksView
import optimus.profiler.ui.NodeTimeLine
import optimus.profiler.ui.NodeTimeLineView
import optimus.profiler.ui.NodeUI
import optimus.profiler.ui.ValueInspector
import optimus.profiler.ui.browser.NodeTreeBrowser
import optimus.utils.misc.Color

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DebuggerUI extends Log {
  val isWindows: Boolean = System.getProperty("os.name").toLowerCase().contains("win")
  private var dbgStop: CountDownLatch = _
  private[optimus] var OnDebuggerStopped: ActionListener = _
  private[optimus] def isDebuggerStopped = dbgStop ne null
  private def setDebuggerStop(bp: CountDownLatch): Unit = {
    dbgStop = bp
    if (OnDebuggerStopped ne null)
      OnDebuggerStopped.actionPerformed(null)
  }

  val callbacks = mutable.HashMap.empty[String, Any => Any]
  def callback(name: String, x: Any): Any = {
    callbacks.get(name).foreach { f =>
      f.synchronized(f(x))
    }
  }
  def addCallback[X, Y](name: String)(f: X => Y): Unit = callbacks.put(name, f.asInstanceOf[Any => Any])

  def addCollectNodes(name: String): ArrayBuffer[(NodeTask, Any)] = {
    val nodes = ArrayBuffer.empty[(NodeTask, Any)]
    addCallback(name) { x: Any =>
      nodes += ((EvaluationContext.currentNode, x))
      nodes.size
    }
    nodes
  }

  // for NodeMenu (ie, inline nodes in browser)
  var _maxCharsInInlineArg = 80
  var maxCharsInInlineArg: Int = _maxCharsInInlineArg // keep the maximum character settings

  var brkCount = 0

  // Before each callback these are set
  var ntsk: NodeTask = _ // On callback set to the current task

  /** Set by user via GUI and used REPL before executing any command */
  var replScenarioStack: ScenarioStack = _

  /** Set by user via GUI */
  var xval_any: Object = _
  var whatIfNode: NodeTask = _
  var whatIfTweakKey: PropertyNode[_] = _
  var diffSelectionA: NodeTask = _ // TODO (OPTIMUS-43990): Refactor to PNodeTask
  val selectedNodes: ArrayBuffer[PNodeTask] = ArrayBuffer[PNodeTask]()
  var selectedTaskInfo: PNodeTaskInfo = _
  val loadedTraces = new ArrayBuffer[OGTraceReader]()

  /**
   * Used for node result grouping. NOTE: This var is used inside a REPL script, so please be cautious before renaming
   * it or moving it somewhere else. [SEE_SET_CURR_RES_LAMBDA].
   */
  var currResultLambda: AnyRef => AnyRef = _

  def nodeResultType(tasks: List[PNodeTask]): Class[_ <: AnyRef] =
    tasks.toArray.map(_.resultKey.getClass).distinct match {
      case Array(tpe) => tpe
      case _          => throw new Exception("All tasks must have the same result type")
    }

  var custom_formatLine: NodeTask => String = _

  def brk(): Unit = brk("\nStopped: " + (if (ntsk ne null) ntsk.toString else "") + " \"go\" to continue\n")

  def alert(msg: String): Unit = {
    GraphDebuggerUI.showMessage(msg)
    GraphConsole.out(msg)
    log.info(msg) // need to make sure it's on the console, process may have exited by now
  }

  def brk(msg: String): Unit = {
    brkCount += 1
    alert(msg)
    setDebuggerStop(new CountDownLatch(1))
    dbgStop.await()
    setDebuggerStop(null)
    if (OnDebuggerStopped ne null) OnDebuggerStopped.actionPerformed(null)
  }

  def go(): Unit = {
    GraphConsole.out("\n")
    sgo()
  }
  def sgo(): Unit = {
    if (DiagnosticSettings.outOfProcess && JMXConnection.graph.getConsoleFlag) JMXConnection.graph.go()
    if (dbgStop ne null) dbgStop.countDown()
  }

  def bookmark(name: String, color: Color): Unit = {
    BookmarkCounter.report(name, color.getRGB)
    log.info(s"Bookmark ($color): $name")
    out(s"${patch.MilliInstant.now} Bookmark ($color): $name")
  }

  def title: String = GraphDebuggerUI.getTitle
  def title_=(title: String): Unit = GraphDebuggerUI.setTitle(title)

  def out(o: AnyRef): Unit = GraphConsole.out(if (o eq null) "null" else o.toString)

  /** Just method to set breakpoint on for some Graph Debugger operations */
  def debugSink(a: AnyRef, b: AnyRef): Unit = {
    println("debugSink called")
  }

  def printGivenLocations(sn: NodeTask): Unit = {
    val title = "\ngiven() locations\n"
    DebuggerUI.out(title)
    System.out.print(title)
    var ss = sn.scenarioStack()
    while (ss != null && !ss.isRoot) {
      val usage = ss.givenBlockProfile
      if (usage != null) {
        val str = usage.toString + "\n"
        DebuggerUI.out(str)
        System.out.print(str)
      }
      ss = ss.parent
    }
  }

  def debugLabel(debugString: String): Option[NodeTask] = {
    val trimmedString: String = debugString.trim
    MarkObject.getMarkedObjectByLabel(trimmedString.stripSuffix("_DebugLabel"))
  }

  def browse(title: String, filter: Predicate[NodeTask]): Unit = {
    browse(NodeTrace.getTraceBy(filter, false), title)
  }

  def browse(list: JArrayList[PNodeTask], title: String = "Custom Node List", ignoreFilter: Boolean = false): Unit =
    NodeUI.invokeLater {
      GraphDebuggerUI.addTab(title, new GraphBrowser(list, ignoreFilter))
    }

  def getSelectedNodes(minSelf: Long = 0L, minTime: Long = 0L, nMax: Int = 1000): Seq[PNodeTask] =
    getNodesInRange(NodeTimeLine.lastRangeStartAbsNanos, NodeTimeLine.lastRangeEndAbsNanos, minSelf, minTime, nMax)

  def getNodesInRange(t1: Long, t2: Long, minSelf: Long = 0L, minTime: Long = 0L, nMax: Int = 1000): Seq[PNodeTask] = {
    def predicate(ntsk: NodeTask): Boolean =
      ntsk.getCompletedTime >= t1 && ntsk.getFirstStartTime <= t2 && ntsk.getSelfTime >= minSelf && ntsk.getSelfPlusANCTime >= minTime

    val al: JArrayList[PNodeTask] = NodeTrace.getTraceBy(predicate, false, nMax)
    al.asScalaUnsafeImmutable
  }

  final case class NodeSummary(self: Long, total: Long) {
    def +(ntsk: NodeTask): NodeSummary = NodeSummary(self + ntsk.getSelfTime, total + ntsk.getSelfPlusANCTime)
  }
  object NodeSummary {
    def apply(ntsk: NodeTask): NodeSummary = NodeSummary(ntsk.getSelfTime, ntsk.getSelfPlusANCTime)
  }
  def summarizeSelectedNodes(
      minSelf: Long = 0L,
      minTime: Long = 0L,
      nMax: Int = 1000): Map[NodeTaskInfo, NodeSummary] = {
    var n = 0
    val summary = mutable.HashMap.empty[NodeTaskInfo, NodeSummary]
    val t1 = NodeTimeLine.lastRangeStartAbsNanos
    val t2 = NodeTimeLine.lastRangeEndAbsNanos

    def visit(ntsk: NodeTask): Boolean = {
      if (
        ntsk.getCompletedTime >= t1 && ntsk.getFirstStartTime <= t2 && ntsk.getSelfTime >= minSelf && ntsk.getSelfPlusANCTime >= minTime
      ) {
        val nti = ntsk.executionInfo()
        summary.get(nti) match {
          case Some(ns) => summary += nti -> (ns + ntsk)
          case None =>
            n += 1
            summary += nti -> NodeSummary(ntsk)
        }
      }
      n <= nMax
    }

    NodeTrace.visitTraces(visit)
    summary.toMap
  }

  def x2t(x: Double): Long = NodeTimeLine.timeLabelToNanos(x)
  def t2x(t: Long): Double = NodeTimeLine.nanosToTime(t)

  def browseCache(title: String, filter: PropertyNode[_] => Boolean): Unit =
    browse(getPropertyNodesInCaches(filter), title)

  private[profiler] def getPropertyNodesInCaches(
      filter: PropertyNode[_] => Boolean,
      showInternal: Boolean = GraphDebuggerUI.showInternal.get): JArrayList[PNodeTask] = {
    val caches = Caches.allCaches(
      includeSiGlobal = true,
      includeGlobal = true,
      includeNamedCaches = true,
      includePerPropertyCaches = true)
    // for some proxies we will have cacheUnderlyingNode report another node already in the cache, so we dedup
    val map = new Int2ObjectOpenHashMap[PNodeTask]()
    caches.foreach {
      _.foreach(candidate => {
        if (candidate != null && filter(candidate)) {
          val toAdd =
            if (showInternal) Some(candidate)
            else if (candidate.isStarted) Some(candidate.cacheUnderlyingNode)
            else None

          toAdd.foreach(cand => {
            map.putIfAbsent(
              cand.getId,
              NodeTrace.accessProfile(cand)
            )
          })
        }
      })
    }

    val out = new JArrayList[PNodeTask](map.size())
    map.int2ObjectEntrySet().fastForEach(k => out.add(k.getValue))
    // inplace sort by incrementing id so that the nodes end up in approx order of execution
    out.sort((t1: PNodeTask, t2: PNodeTask) => Integer.compare(t1.id(), t2.id()))
    out
  }

  def diff(a: NodeTask, b: NodeTask): Unit =
    GraphDebuggerUI.addTab("Diffs", new ValueInspector(Array(a, b), a.scenarioStack))

  def inspect(values: Any*): Unit = GraphDebuggerUI.addTab("Values", new ValueInspector(values.toArray, null))

  def whatIf(tweakTo: Any): Unit = if ((whatIfNode ne null) && (whatIfTweakKey ne null)) {
    val tweak = SimpleValueTweak[Any](whatIfTweakKey)(tweakTo)
    val nss = whatIfNode.scenarioStack.createChild(Scenario(tweak), DebuggerUI.this)
    val nntsk = whatIfNode.prepareForExecutionIn(nss)
    underStackOf(nntsk.scenarioStack()) { EvaluationContext.enqueueAndWait(nntsk) }
    val task = NodeTrace.accessProfile(nntsk)
    GraphDebuggerUI.addTab("Children of " + NodeFormatUI.formatName(nntsk), NodeTreeBrowser(task))
  }

  def collectTweaks(ntsk: NodeTask, limit2parentScenarioStacks: Boolean): JArrayList[NodeTask] = {
    val ss = ntsk.scenarioStack
    val tweaks = new JArrayList[NodeTask]
    foreachDescendant(NodeTrace.accessProfile(ntsk))((c, _) => {
      c.getTask match {
        case tn: TweakNode[_] =>
          if (!limit2parentScenarioStacks || tn.trackingDepth <= ss.trackingDepth)
            tweaks.add(tn)
        case _ =>
      }
    })
    tweaks
  }

  /* It's debug/test only because it flattens the tweaks AND depends on infectionSet debug flag
  def usedTweaksOf(ntsk: NodeTask) : JHashSet[Tweak] = {
    val sbs = ntsk.getTweakInfection
    val r = new JHashSet[Tweak]
    if (sbs ne null) {
      var curr = ntsk.scenarioStack()
      while ((curr ne null) && !curr.isRoot) { // need cacheID rather than scenario.allTweaks because we should report dependencies on expanded tweaks
        val it = curr._cacheID.allTweaksAsIterable.iterator
        while (it.hasNext) {
          val tweak = it.next
          if (sbs.contains(tweak.id)) r.add(tweak)
        }
        curr = curr.parent
      }
    }
    r
  }
   */

  /**
   * It's debug/test only because depends on infectionSet debug flag XS Records Tweaks based on hierarchical usage so
   * can given scenario stack as follows
   * {{{
   *  given(e.x:=3) {
   *    given(E.y := { x + 1 }, E.z:= { x + 1 } ) {
   *      given(e.x = 4) { x + y + z }
   *    }
   *  }
   * }}}
   * This is the result:
   * {{{
   *  e.x := 4
   *    E.y := { x + 1 }, E.z := { x + 1 }
   *      e.x := 3
   * }}}
   * This basically models the original scenario stack, culling out the unused tweaks
   * And it's useful for equality check!
   * Note: This is not as powerful as the XS [[RecordedTweakables]] which is more precise in its dependency trees
   */
  def minimumScenarioOf(ntsk: NodeTask): Scenario = {
    if (ntsk.isXScenarioOwner) {
      Scenario.empty // We don't want to fail, but XS should be checked outside
      // Also possible extension in the future would be something like this:
      // ntsk.scenarioStack.tweakableListener.asInstanceOf[RecordedTweakables].asScenario
    } else {
      val sbs = ntsk.getTweakInfection
      var r = Scenario.empty
      if (sbs ne null) {
        var curr = ntsk.scenarioStack()
        while ((curr ne null) && !curr.isRoot) {
          val tweaks = ArrayBuffer[Tweak]()
          // need cacheID rather than scenario.allTweaks because we should report dependencies on expanded tweaks
          val it = curr._cacheID.allTweaksAsIterable.iterator
          while (it.hasNext) {
            val tweak = it.next()
            if (sbs.contains(tweak.id)) tweaks += tweak
          }
          if (tweaks.nonEmpty)
            r = Scenario(tweaks).nest(r)
          curr = curr.parent
        }
      }
      r
    }
  }

  def collectTweaksAsTweakTreeNode(task: PNodeTask): RecordedTweakables = {
    val ss = task.scenarioStack
    val ihm = new JIdentityHashMap[PNodeTask, PNodeTask]()

    def visit(tsk: PNodeTask, rtweaks: mutable.HashMap[PropertyNode[_], TweakTreeNode]): Unit = {
      if (!ihm.containsKey(tsk)) {
        ihm.put(tsk, task)
        val children = tsk.getCallees
        while (children.hasNext) {
          val child = children.next()
          child.getTask match { // consider making this work for recorded (non-live) data
            case tn: TweakNode[_] =>
              val recordedTweaks = new mutable.HashMap[PropertyNode[_], TweakTreeNode]()
              visit(child, recordedTweaks)
              val rt = RecordedTweakables(recordedTweaks.values.toArray)
              if (tn.trackingDepth <= ss.trackingDepth)
                rtweaks.put(tn, tn.toTweakTreeNode(rt))
            case _ =>
              visit(child, rtweaks)
          }
        }
      }
    }

    try {
      val recordedTweaks = new mutable.HashMap[PropertyNode[_], TweakTreeNode]()
      visit(task, recordedTweaks)
      RecordedTweakables(recordedTweaks.values.toArray)
    } catch {
      case e: Exception =>
        System.err.println(e.getMessage)
        RecordedTweakables.empty
    }
  }

  /** Flattens scenario stack into ArrayBuffer */
  def scenarioStackFlat(ss: ScenarioStack): ArrayBuffer[ScenarioStack] = {
    val ssf = new ArrayBuffer[ScenarioStack]() // stacks
    var css = ss
    while ((css ne null) && !css.isRoot) {
      ssf += css
      css = css.parent
    }
    ssf
  }

  /** Returns the first common parent index and -1 if roots are different */
  def getCommonScenarioStackDepth(ss1: ScenarioStack, ss2: ScenarioStack): Int = {
    var r = -1 // A sentinel to show that they have nothing in common
    if ((ss1.ssShared eq null) || (ss2.ssShared eq null)) return r // tweak ACPNs can have ssShared eq null
    if (ss1.ssShared._cacheID eq ss2.ssShared._cacheID) {
      val ss1f = scenarioStackFlat(ss1)
      val ss2f = scenarioStackFlat(ss2)
      var i = Math.min(ss1f.length, ss2f.length)
      while (i > 0 && r < 0) {
        if (ss1f(ss1f.length - i) == ss2f(ss2f.length - i)) r = i
        i -= 1
      }
      if (r < 0) r = 0 // The case where only root is the same
    }
    r
  }

  /**
   * An evil version of EvaluationContext.given used by the graph profiler to compute things while completely
   * disregarding SI etc.
   */
  final def underStackOf[T](ssIn: ScenarioStack)(f: => T): T = {
    if (!SchedulerIsInitialized.ready) return f

    var r: T = null.asInstanceOf[T]
    var saved_ss: ScenarioStack = null
    var cn: NodeTask = null
    try {
      // Special case some scenarios (constance and si)
      // under which some code may throw. We don't need that verification here
      val ss =
        if ((ssIn eq null) || ssIn.isConstant) ScenarioState.minimal.scenarioStack
        else ssIn.asVeryBasicScenarioStack

      if (!EvaluationContext.isInitialised && (ss.ssShared ne null))
        EvaluationContext.initialize(ScenarioState(ss.ssShared.scenarioStack))

      cn = EvaluationContext.currentNode
      saved_ss = cn.scenarioStack()
      cn.replace(ss.asBasicScenarioStack.withCancellationScopeRaw(CancellationScope.newScope()))
      r = f
    } catch {
      case _: Throwable =>
    }

    if (cn != null) cn.replace(saved_ss)
    r
  }

  private def foreachDirectRelative[T](tsk: T)(getRelatives: T => Seq[T])(f: T => Unit): Unit = {
    try {
      getRelatives(tsk).foreach(f)
    } catch {
      case e: Exception => System.err.println(e.getMessage)
    }
  }

  private def foreachRelative[T](initTsk: T)(getRelatives: T => Seq[T], maxLevel: Int, afterVisit: Boolean)(
      f: (T, Int) => Unit): Unit = {

    val ihm = new JIdentityHashMap[T, T]()

    def visit(tsk: T, level: Int): Unit = {
      if (!ihm.containsKey(tsk)) {
        ihm.put(tsk, initTsk)
        if (!afterVisit) f(tsk, level)
        if (level < maxLevel) {
          getRelatives(tsk).foreach {
            visit(_, level + 1)
          }
        }
        if (afterVisit) f(tsk, level)
      }
    }

    try {
      visit(initTsk, 0)
    } catch {
      case e: Exception => System.err.println(e.getMessage)
    }
  }

  private type SF = Function[Seq[PNodeTask], Seq[PNodeTask]]

  def foreachAncestor(initTsk: PNodeTask, sortFn: SF = identity, after: Boolean = false, maxLevel: Int = 1000)(
      f: (PNodeTask, Int) => Unit): Unit =
    foreachRelative(initTsk)(n => sortFn(n.getCallers.asScala.to(ArraySeq)), maxLevel, after)(f)

  def ancestors(
      initTsk: PNodeTask,
      maxLevel: Int = 1000,
      sortFn: SF = identity,
      after: Boolean = false): Seq[(PNodeTask, Int)] = {
    val res = ArraySeq.newBuilder[(PNodeTask, Int)]
    foreachAncestor(initTsk, sortFn, after, maxLevel) { (n, i) =>
      res += ((n, i))
    }
    res.result()
  }

  def foreachDescendant(initTsk: PNodeTask, sortFn: SF = identity, after: Boolean = false, maxLevel: Int = 1000)(
      f: (PNodeTask, Int) => Unit): Unit =
    foreachRelative(initTsk)(n => sortFn(n.getCallees.asScala.to(ArraySeq)), maxLevel, after)(f)

  def descendants(
      initTsk: PNodeTask,
      maxLevel: Int = 1000,
      sortFn: SF = identity,
      after: Boolean = false): Seq[(PNodeTask, Int)] = {
    val res = ArraySeq.newBuilder[(PNodeTask, Int)]
    foreachDescendant(initTsk, sortFn, after, maxLevel) { (n, i) =>
      res += ((n, i))
    }
    res.result()
  }

  def foreachParent(tsk: PNodeTask)(f: PNodeTask => Unit): Unit =
    foreachDirectRelative(tsk)(_.getCallers.asScala.to(ArraySeq))(f)
  def foreachChild(tsk: PNodeTask)(f: PNodeTask => Unit): Unit =
    foreachDirectRelative(tsk)(_.getCallees.asScala.to(ArraySeq))(f)

  def hasAncestor(tsk: PNodeTask, i: NodeTaskInfo): Boolean = ancestors(tsk).exists(_._1.infoId == i.profile)
  def haveAncestor(tsks: Iterable[PNodeTask], i: NodeTaskInfo): Int = tsks.count(t => hasAncestor(t, i))

  /** Loads trace files and optionally adds tabs for displaying the data */
  def loadTrace(fileName: String, showTimeLine: Boolean, showHotspots: Boolean, showBrowser: Boolean): OGTraceReader = {
    val file = new File(fileName)
    val startTime = System.nanoTime()
    println(s"Loading $fileName")
    val reader = TraceHelper.getReaderFromFile(fileName)
    println(s"Finished: ${(System.nanoTime() - startTime) * 1e-6} ms")
    loadTrace(reader, showTimeLine, showHotspots, showBrowser, file.getName)
  }

  def loadTrace(
      reader: OGTraceReader,
      showTimeLine: Boolean,
      showHotspots: Boolean,
      showBrowser: Boolean,
      tabName: String): OGTraceReader = {
    if (showTimeLine && reader.hasSchedulerTimes)
      GraphDebuggerUI.addTab("Time Line [" + tabName + "]", new NodeTimeLineView(reader))
    if (showHotspots)
      GraphDebuggerUI.addTab("Profiler [" + tabName + "]", new NodeProfiler(reader))
    if (showBrowser && reader.hasRecordedTasks) {
      val browser = new GraphBrowser(
        reader.getRawTasks(
          true,
          Debugger.dbgOfflineShowStart.get,
          Debugger.dbgOfflineShowUnattached.get,
          Debugger.dbgOfflineShowSpecProxies.get,
          Debugger.dbgOfflineShowNonCacheable.get
        )
      )
      GraphDebuggerUI.offlineBrowsers.add(browser)
      GraphDebuggerUI.addTab(
        "Browser [" + tabName + "]",
        browser,
        () => GraphDebuggerUI.offlineBrowsers.remove(browser))
    }

    loadedTraces += reader
    reader
  }

  def compareNodeStacks(org: OGTraceReader, compareTo: OGTraceReader): Unit = {
    val nodeStacksView = new NodeStacksView(org, compareTo)
    nodeStacksView.cmdRefresh()
    GraphDebuggerUI.addTab("Compared NodeStacks", nodeStacksView)
  }

  def previewConfig(reader: OGTraceReader, config: String, tabName: String): Unit = {
    val profiler = new NodeProfiler(reader)
    profiler.cmdRefreshPreviewConfig(config)

    GraphDebuggerUI.addTab("Profiler [" + tabName + "]", profiler)
  }

  def loadGroupConfigPreviewer(tabName: String): GroupConfigs = {
    val groupConfigViewer = new GroupConfigs()
    GraphDebuggerUI.addTab("Group Analysis [" + tabName + "]", groupConfigViewer)
    groupConfigViewer
  }

  def previewConfig(
      reader: OGTraceReader,
      groupConfigPreviewer: GroupConfigs,
      config: String,
      tabName: String): Unit = {
    val profiler = new NodeProfiler(reader)
    profiler.cmdRefreshPreviewConfig(config)

    groupConfigPreviewer.addTab(tabName, profiler)
  }
}
