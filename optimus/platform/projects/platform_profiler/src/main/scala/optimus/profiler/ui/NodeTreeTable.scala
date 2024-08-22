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
package optimus.profiler.ui

import optimus.graph.DiagnosticSettings

import java.awt.Font
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.util
import java.util.{Collection => JCollection}
import java.util.{Iterator => JIterator}
import javax.swing.Icon
import javax.swing.ImageIcon
import javax.swing.table.TableCellRenderer
import optimus.graph.InstancePropertyTarget
import optimus.graph.NodeTask
import optimus.graph.NodeTrace
import optimus.graph.OGSchedulerTimes
import optimus.graph.OGTrace
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.Tweak
import optimus.profiler.DebuggerUI
import optimus.profiler.Generic
import optimus.profiler.NodeFormatUI
import optimus.profiler.extensions.NPTreeNodeExt
import optimus.profiler.recipes.NodeGraphVisitor
import optimus.profiler.recipes.NodeGraphVisitor.NodeToUTrackToInvalidate
import optimus.profiler.ui.NPTableRenderer.TimeFullRange
import optimus.profiler.ui.NPTableRenderer.TimeSubRange
import optimus.profiler.ui.NodeTreeTable.ExpandConfig
import optimus.profiler.ui.NodeTreeTable.recomputeMinMaxTime
import optimus.profiler.ui.common.NodeGroup

import java.awt.Color
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

/** Browse children/parents of traced nodes */
object NodeTreeTable {
  private[optimus] val idColumnName = "ID"
  private[optimus] val selfTimeColumnName = "Self Time (ms)"

  private[ui] val exceptionFilter = "[EX]"

  final case class Mode(
      name: String,
      toolTip: String,
      directionToChildren: Boolean = true,
      autoExpand: Boolean = true,
      icon: Icon = null)
  final case class ExpandConfig(skipInternal: Boolean = false, mode: Mode) {
    def autoExpand: Boolean = mode.autoExpand
    def directionToChildren: Boolean = mode.directionToChildren

    def withDirection(toChildren: Boolean): ExpandConfig =
      if (directionToChildren != toChildren) copy(mode = mode.copy(directionToChildren = toChildren)) else this
  }

  private val uiInvalidationToolTip =
    "Shows UI elements that used the values of the selected node(s) and the tweak that caused the invalidation"
  private val uiInvalidationByTweakToolTip =
    "Shows UI elements that used values of the selected node(s) and groups by the tweak that caused the invalidation"

  private val cp = "Shows common parent of top two selected nodes"

  val children: Mode = Mode("Children", "Shows the children/callees", directionToChildren = true, icon = Icons.callee)
  val parents: Mode = Mode("Parents", "Shows the parents/callers", directionToChildren = false, icon = Icons.caller)
  val uiInvalidation: Mode =
    Mode("UI Invalidation", uiInvalidationToolTip, autoExpand = false, icon = Icons.uiInvalidates)
  val uiInvalidationGroupByTweak: Mode =
    Mode("UI Invalidation by Tweak", uiInvalidationByTweakToolTip, autoExpand = false, icon = Icons.uiInvalidates)
  val commonParent: Mode =
    Mode("Common Parent", cp, autoExpand = true, directionToChildren = true, icon = Icons.commonParent)

  val modes = List(children, parents, null /* separator */, uiInvalidation, uiInvalidationGroupByTweak, commonParent)
  val noAutoExpandConfig: ExpandConfig = ExpandConfig(skipInternal = true, uiInvalidation)

  val fakePNodeTask: PNodeTask = PNodeTask.fake

  private val allColumns = ArrayBuffer(
    new SortableTableColumn[NodeTreeView, NodeTreeView]("Name", 600) {
      override def valueOf(row: NodeTreeView): NodeTreeView = row
      override def valueOf(row: NodeTreeView, row2: NodeTreeView): NodeTreeView = valueOf(row)
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.treeRenderer
      override def parsedFilter(filter: String): (NodeTreeView, NodeTreeView) => Boolean = {
        val flt = TableColumn.parseStringFilterEx(filter)
        if (flt eq null) null
        else (row1, row2) => flt(valueOf(row1, row2).title)
      }
      override def allowDeselecting: Boolean = false // not displaying Name column breaks grouping icon
    },
    new TableColumnString[NodeTreeView]("Result", 150) {
      override def valueOf(row: NodeTreeView): String = row.resultAsString
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
    },
    new AggregatableTableColumnTime[NodeTreeView]("Wall Time (ms)", 120) {
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
      override def getValueFromTask(task: PNodeTask): Double = task.wallTime() * 1e-6
    },
    new TableColumnTime[NodeTreeView]("Start Time (ms)", 120) {
      override def valueOf(row: NodeTreeView): Double = {
        val firstStartTime = row.task.firstStartTime
        if (firstStartTime == 0) 0.0 // Just no data
        else if (NodeTimeLine.lastRangeStartAbsNanos != 0)
          (firstStartTime - NodeTimeLine.lastRangeStartAbsNanos) * 1e-6
        else if (DebuggerUI.diffSelectionA ne null)
          (firstStartTime - DebuggerUI.diffSelectionA.getFirstStartTime) * 1e-6
        else
          (firstStartTime - OGSchedulerTimes.getFirstGraphEnterTime) * 1e-6
      }
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
      override def toolTip: String =
        "Start time (ms) relative to selected node or start of the selected range in the timeline (therefore may be negative)"
    },
    new AggregatableTableColumnTime[NodeTreeView](selfTimeColumnName, 120) {
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
      override def getValueFromTask(task: PNodeTask): Double = task.selfTime * 1e-6
    },
    new TableColumn[NodeTreeView]("Entity ", 400) {
      override def valueOf(row: NodeTreeView): Any = row.task.getEntity
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
    },
    new TableColumnString[NodeTreeView]("Property Flags", 100) {
      override def valueOf(row: NodeTreeView): String = row.task.propertyFlagsAsString
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
    },
    new TableColumnString[NodeTreeView]("Node Flags", 100) {
      override def valueOf(row: NodeTreeView): String = row.task.stateAsString
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
    },
    new TableColumnCount[NodeTreeView](idColumnName, 100) {
      override def valueOf(row: NodeTreeView): Int = row.task.id
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
    },
    new TableColumnString[NodeTreeView]("Depends On", 100) {
      override def valueOf(row: NodeTreeView): String = row.task.dependsOnTweakableString
    },
    new TableColumnString[NodeTreeView]("Process", 100) {
      override def valueOf(row: NodeTreeView): String = row.task.process.displayName()
    },
    new TableColumnCount[NodeTreeView]("Property Starts", 100) {
      override def valueOf(row: NodeTreeView): Int = {
        if (row.task.isLive) {
          val infoID = row.task.infoId()
          val pnti = OGTrace.liveReader().getTaskInfo(infoID)
          pnti.start.toInt
        } else
          0
      }
      override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
    }
  )

  private def getTreeSelector(selectionPath: String): TreeSelector[NodeTreeView] = {
    val matchExp = new ArrayBuffer[PNodeTask => Boolean]()
    val matchCond = new ArrayBuffer[Char]()

    // filter on exceptions..
    if (selectionPath == exceptionFilter) {
      matchExp += null
      matchCond += 'X'
    }
    val usePath = ("." + selectionPath).split("/")

    def isSlash(idx: Int) = if (usePath.length > idx) usePath(idx).isEmpty else false

    var i = 1 // Eat the first '.'
    while (i < usePath.length) {
      if (isSlash(i) && !isSlash(i + 1)) {
        matchExp += NodeFormatUI.filterNodesFastWithMemory(usePath(i + 1))
        matchCond += 'D' // Deep match
        i += 2
      } else if (!isSlash(i)) {
        matchExp += NodeFormatUI.filterNodesFastWithMemory(usePath(i + 1))
        matchCond += 'E' // Exact match
        i += 1
      } else {
        throw new Exception("Syntax unsupported ///")
      }
    }

    class TS(val level: Int, val visitedNodes: ArrayBuffer[PNodeTask]) extends TreeSelector[NodeTreeView] {
      var matched = false // Complete match
      var maybeLower = true
      var levelMatched = false // match on this level

      override def review(i: NodeTreeView): Unit = {
        // if matchCond is empty, we were just filtering for exceptions and don't care about depth
        def deepMatch(level: Int): Boolean = matchCond.isEmpty || matchCond(level) == 'D'

        def hasChildrenWithExceptions: Boolean = {
          i.hasChildren && i.getUncompressedChildren.exists(_.asInstanceOf[NodeTreeView].task.isDoneWithException)
        }

        val pnt = i.task
        if (pnt.visitedID > 0) {
          matched = false
          maybeLower = false
        } else {
          pnt.visitedID = 1
          visitedNodes += i.task
          if (level < matchExp.size) {
            if (matchCond(level) == 'X') {
              matched = i.task.isDoneWithException && !hasChildrenWithExceptions
              maybeLower = true
            } else {
              levelMatched = matchExp(level)(i.task)
              if (level == matchExp.size - 1) {
                maybeLower = !levelMatched && deepMatch(level)
                matched = levelMatched
              } else {
                matched = false // Not a last level so for sure not a 'full' match
                maybeLower = levelMatched || deepMatch(level)
              }
            }
          } else {
            matched = false
            maybeLower = false
          }
        }
      }

      def moveDown() = new TS(if (levelMatched) level + 1 else level, visitedNodes)
      override def destroy(): Unit = {
        val it = visitedNodes.iterator
        while (it.hasNext) it.next().visitedID = 0
      }
    }
    new TS(0, new ArrayBuffer[PNodeTask]())
  }

  def recomputeMinMaxTime(firstAndLastTimes: Seq[(Long, Long)]): TimeFullRange = {
    var minTime = Long.MaxValue
    var maxTime = Long.MinValue
    var i = 0
    while (i < firstAndLastTimes.length) {
      val (firstStartTime, completedTime) = firstAndLastTimes(i)
      minTime = Math.min(minTime, firstStartTime)
      maxTime = Math.max(maxTime, completedTime)
      i += 1
    }
    TimeFullRange(minTime, maxTime, NodeTimeLine.lastRangeStartAbsNanos, NodeTimeLine.lastRangeEndAbsNanos)
  }
}

/** Browse children/parents of traced nodes */
class NodeTreeTable(var _roots: ArrayBuffer[PNodeTask], val details: NodeView, var expandCfg: ExpandConfig)
    extends NPTreeTable[NodeTreeView] {
  emptyRow = new NodeTreeView(PNodeTask.fake, expandCfg, -1)

  private var fullRange: TimeFullRange = _

  def this(details: NodeView, expandCfg: ExpandConfig) = this(ArrayBuffer.empty[PNodeTask], details, expandCfg)

  override protected def afterUpdateList(): Unit = {
    fullRange = null
    super.afterUpdateList()
  }

  private val localColumns = ArrayBuffer(new TableColumnTimeRange[NodeTreeView]("Time Frame", 100) {
    override def valueOf(row: NodeTreeView): TimeSubRange = {
      val firstAndLastTimes = mutable.ArrayBuffer[(Long, Long)]()
      if (fullRange eq null) {
        rows.foreach { nodeTreeView =>
          if (nodeTreeView.task ne NodeTreeTable.fakePNodeTask) {
            val pntsk = nodeTreeView.task
            if ((pntsk ne null) && pntsk.firstStartTime > 0)
              firstAndLastTimes += ((pntsk.firstStartTime, pntsk.completedTime))
          }
        }
        fullRange = recomputeMinMaxTime(firstAndLastTimes)
      }
      TimeSubRange(row.task.firstStartTime, row.task.completedTime, fullRange)
    }
    override def computeSummary(table: NPTable[NodeTreeView], indexes: Seq[Int]): AnyRef = "" // Do not aggregate
  })

  setView(NodeTreeTable.allColumns ++ localColumns)

  /** Returns selections ignoring groups */
  final def getNodeTaskSelections: ArrayBuffer[PNodeTask] = {
    getSelections.map(_.task).filterNot(_ eq NodeTreeTable.fakePNodeTask)
  }

  final def selectNode(id: Int): Unit = {
    val matchID = (nv: NodeTreeView) => nv.task.id == id
    openTo(useSelection = false, stopOnFirstMatch = true, expandRoots = false, matchID)
    select(matchID)
  }

  final def showNode(newRootTask: PNodeTask): Unit = {
    showNodes(if (newRootTask eq null) ArrayBuffer.empty[PNodeTask] else ArrayBuffer(newRootTask))
  }

  final def showNodes(newTasks: ArrayBuffer[PNodeTask], expand: Boolean = true): Unit = {
    _roots = if (newTasks eq null) ArrayBuffer.empty[PNodeTask] else newTasks
    if (_roots.nonEmpty) {
      if (expandCfg.mode eq NodeTreeTable.uiInvalidation)
        expandUITracks(_roots) // Expand ui tracks and setList
      else if (expandCfg.mode eq NodeTreeTable.uiInvalidationGroupByTweak)
        expandUITracksGroupedByTweaks(_roots) // Expand ui tracks and setList
      else if (expandCfg.mode eq NodeTreeTable.commonParent)
        expandCommonParent(_roots)
      else
        openFromRoot(_roots, if (!expand) 1 else 50)
    } else setList(List())
  }

  private def openFromRoot(roots: ArrayBuffer[PNodeTask], maxCount: Int = 50): Unit =
    openFromRoot(roots, expandCfg, maxCount)

  private def openFromRoot(roots: ArrayBuffer[PNodeTask], cfg: ExpandConfig, maxCount: Int): Unit = {
    val list = roots.map(root => new NodeTreeView(root, cfg, 0))
    NPTreeNodeExt.openTreeBFS(list, maxCount)
    setList(list: Iterable[NodeTreeView])
  }

  private def expandCommonParent(newTasks: ArrayBuffer[PNodeTask]): Unit = {
    if (newTasks.length >= 2) {
      val taskA = newTasks.head
      val taskB = newTasks(1)
      val parent = NodeGraphVisitor.commonParent(taskA, taskB)
      if (parent ne null) openFromRoot(ArrayBuffer(parent))
      else {
        println(s"No common parent between ${taskA.nodeName} and ${taskB.nodeName}")
        setList(List())
      }
    } else if (newTasks.length == 1)
      openFromRoot(newTasks, expandCfg.withDirection(toChildren = false), maxCount = 50) // same as parents view
  }

  private def expandUITracksGroupedByTweaks(roots: ArrayBuffer[PNodeTask]): Unit = {
    val nuis = NodeGraphVisitor.nodeToUTrackToInvalidate(roots)

    val views = new util.IdentityHashMap[Tweak, InvalidatingTweakTreeView]()
    val it = nuis.iterator
    while (it.hasNext) {
      val nui = it.next()
      val twk = if (nui.invalidatingTweak eq null) Generic.emptyTweak else nui.invalidatingTweak
      var view = views.get(twk)
      // come back to this...
      val instanceTwk = if (twk.target.isInstanceOf[InstancePropertyTarget[_, _]]) twk else Generic.emptyTweak
      if (view eq null) {
        view = new InvalidatingTweakTreeView(instanceTwk, 0)
        views.put(twk, view)
      }
      view.addChild(new UTrackTreeView(nui.utrack, 1))
    }
    setList(views.values.asScala)
  }

  private def expandUITracks(roots: ArrayBuffer[PNodeTask]): Unit = {
    val nuis = NodeGraphVisitor.nodeToUTrackToInvalidate(roots)
    val treeNodes = ArrayBuffer[NodeTreeView]()
    val it = nuis.iterator
    while (it.hasNext) {
      val nui = it.next()
      val utrackView = new UTrackTreeView(nui.utrack, 0)
      utrackView.initTweakAndOriginalNodeChildren(nui)
      utrackView.open = true
      treeNodes += utrackView
    }
    setList(treeNodes: Iterable[NodeTreeView])
  }

  final def reset(): Unit = showNodes(null)

  final def switchMode(mode: NodeTreeTable.Mode): Unit = {
    expandCfg = expandCfg.copy(mode = mode)
    showNodes(_roots)
  }

  final def showInternal(enable: Boolean, expand: Boolean): Unit = {
    expandCfg = expandCfg.copy(skipInternal = !enable)
    showNodes(_roots, expand = expand)
  }

  final def showNodes(expand: Boolean): Unit = showNodes(_roots, expand)

  final val menu = NodeMenu.create(
    new HasSelectedNode[NodeTreeView] {
      override def getSelection: PNodeTask = {
        val sel = getNodeTaskSelections
        if (sel.nonEmpty) sel.head else null
      }
      override def getSelections: Seq[PNodeTask] = getNodeTaskSelections
      override def getSelectionsAsRows: Seq[NodeTreeView] = NodeTreeTable.this.getSelections
      override def getTop: Option[NodeTask] = {
        val rows = NodeTreeTable.this.rows
        if (rows.nonEmpty) Some(rows(0).task.getTask) else null
      }
      override def refresh(): Unit = NodeTreeTable.this.dataTable.repaint()
    },
    this
  )
  dataTable.setComponentPopupMenu(menu)

  dataTable.getSelectionModel.addListSelectionListener(e => {
    if (!e.getValueIsAdjusting && (details ne null)) details.showNodes(getNodeTaskSelections.toArray)
  })

  final def expand(selectionPath: String, useSelection: Boolean, compress: Boolean): Unit = {
    val normalizedPath = {
      if (selectionPath == NodeTreeTable.exceptionFilter) selectionPath
      else if (selectionPath.startsWith("/")) selectionPath
      else "//" + selectionPath
    }
    val treeSelector = NodeTreeTable.getTreeSelector(normalizedPath)
    try { super.openTo(useSelection, treeSelector, compress) }
    finally { treeSelector.destroy() }
  }
}

/** Customized version of NodeTreeTable to be used for selection in TimeLineView */
class NodeTreeTableForTimeLineView extends NodeTreeTable(null, NodeTreeTable.noAutoExpandConfig)

/** Customized version of NodeTreeTable to be used for selection as a grouping pane in GraphBrowser */
class NodeTreeTableForGroupedBrowsing(details: NodeView)
    extends NodeTreeTable(details, NodeTreeTable.noAutoExpandConfig) {
  private var increasingOrder = true

  init()

  /** enable filtering */
  setFilterRow(true)
  override def wantSummary: Boolean = false // note, NPTreeTable disables this, so re-enable here

  /** to allow us to redo grouping in browser */
  private var _browserFilterCallback: Seq[PNodeTask] => Unit = _
  private[ui] def setBrowserFilterCallback(callback: Seq[PNodeTask] => Unit): Unit = _browserFilterCallback = callback

  /** need to setRows on the underlying data model before calling updateFilterAndRefresh, then redo browser grouping */
  private[ui] override def updateFilterAndRefresh(): Unit = {
    super.setRows(prefilter_rows, prefilter_rowsC)
    super.updateFilterAndRefresh()
    if (_browserFilterCallback ne null)
      _browserFilterCallback(rows.map(_.task).filterNot(_ eq PNodeTask.fake))
  }

  private[ui] def filter(filteredTasks: JCollection[PNodeTask]): JCollection[PNodeTask] = {
    val nodeTreeViews = ArrayBuffer[NodeTreeView]()
    val it = filteredTasks.iterator()
    while (it.hasNext) {
      val task = it.next()
      nodeTreeViews += NodeTreeView(task)
    }

    // these won't ever be visible, but set the underlying data model to filter
    super.setRows(nodeTreeViews, rowsC)
    super.updateFilterAndRefresh()

    // get back the filtered tasks to be grouped
    val filtered = new ArrayBuffer[PNodeTask]()
    val it2 = rows.iterator
    while (it2.hasNext) {
      filtered += it2.next().task // no need to filter for fakeTask here because input tasks will never be fake
    }
    filtered.asJavaCollection
  }

  private def sortChildren(node: NPTreeNode, colNum: Int): Unit = {
    val nodeGroup = node.asInstanceOf[NodeTreeViewGroup]
    val children = dataTable.getTCol(colNum).sort(nodeGroup.getChildren, increasingOrder)
    nodeGroup.setChildren(children.asInstanceOf[ArrayBuffer[NodeTreeView]])
  }

  private def sort(roots: Iterable[NPTreeNode], colNum: Int): Unit = {
    def sortAllChildren(treeNodes: Iterable[NPTreeNode]): Unit = {
      val it = treeNodes.iterator
      while (it.hasNext) {
        val ssu = it.next()
        if (ssu ne null) {
          if (ssu.hasChildren)
            ssu.getChildren.head match {
              case _: NodeTreeViewGroup => sortAllChildren(ssu.getChildren)
              case _: NodeTreeView      => sortChildren(ssu, colNum)
              case _                    =>
            }
        }
      }
    }
    sortAllChildren(roots)
  }

  private[ui] def setListBySorting(roots: Iterable[NodeTreeView], increasingOrder: Boolean, colNum: Int): Unit = {
    sort(roots, colNum)
    setList(roots)
    dataTable.model.fireTableDataChanged()
    sumTable.model.fireTableDataChanged()
  }

  private def init(): Unit = {
    headerTable.getTableHeader.addMouseListener(new MouseAdapter {
      override def mouseClicked(e: MouseEvent): Unit = {
        val col = dataTable.columnAtPoint(e.getPoint)
        increasingOrder = !increasingOrder
        setListBySorting(roots, increasingOrder = increasingOrder, col)
      }
    })
  }

  /** For testing purpose only */
  private[ui] def getRowsForTesting: ArrayBuffer[NodeTreeView] = rows.clone()
}

object NodeTreeView {
  def apply(task: PNodeTask): NodeTreeView = new NodeTreeView(task, NodeTreeTable.noAutoExpandConfig, 0)
}

class NodeTreeView(override val task: PNodeTask, val expandCfg: ExpandConfig, val level: Int)
    extends NPTreeNode
    with DbgPrintSource
    with Ordered[NodeTreeView] {
  private var hasCompressedChildren: Boolean = _
  protected var children: ArrayBuffer[NodeTreeView] = _

  def setChildren(children: ArrayBuffer[NodeTreeView]): Unit = this.children = children

  def this(node: NodeTask, expandCfg: ExpandConfig, level: Int) = this(NodeTrace.accessProfile(node), expandCfg, level)

  def compare(that: NodeTreeView): Int = this.title.compare(that.title)

  override def foreground: Color = if (task.isDoneWithException) Color.RED else super.background

  private def nodeChildren: JIterator[PNodeTask] = {
    if (expandCfg.directionToChildren) {
      if (!DiagnosticSettings.showEnqueuedNotCompletedNodes || task.isDone) task.getCallees
      else task.getCalleesWithEnqueuesWhenNotDone
    } else task.getCallers.iterator
  }

  private def fillWithChildren(r: ArrayBuffer[NodeTreeView]): Unit = {
    val it = nodeChildren
    while (it.hasNext) {
      val child = it.next()
      if (expandCfg.skipInternal && child.isInternal) {
        val childRow = new NodeTreeView(child, expandCfg, level) // Note: same level (we are inlining)
        if (childRow.hasChildren)
          childRow.fillWithChildren(r)
      } else
        r += new NodeTreeView(child, expandCfg, level + 1)
    }
  }

  def addChild(child: NodeTreeView): Unit = {
    if (children eq null) children = new ArrayBuffer[NodeTreeView]()
    children += child
  }

  def getChildren: Iterable[NPTreeNode] = {
    if (children == null) {
      if (expandCfg.autoExpand && hasChildren) {
        children = new ArrayBuffer[NodeTreeView]()
        fillWithChildren(children)
      } else
        children = ArrayBuffer.empty[NodeTreeView]
    }
    children
  }

  override def getUncompressedChildren: Iterable[NPTreeNode] = {
    if (hasCompressedChildren) {
      hasCompressedChildren = false
      children = null
    }
    getChildren
  }

  /* Replace the sequence of non-matched nodes with a single expandable compressed node */
  override def compressClosed(): Unit = {
    val newChildren = new ArrayBuffer[NodeTreeView]
    var fakeNode: NodeTreeViewCompressed = null
    var i = 0
    while (i < children.length) {
      val child = children(i)
      if (!(child.open || child.matched)) {
        if (fakeNode == null) fakeNode = new NodeTreeViewCompressed(expandCfg, level + 1)
        fakeNode.children += child
      } else {
        if (fakeNode != null) newChildren += fakeNode
        fakeNode = null
        newChildren += child
      }
      i += 1
    }
    if (fakeNode != null) newChildren += fakeNode
    hasCompressedChildren = true
    children = newChildren
  }

  override def hasChildren: Boolean = {
    ((children ne null) && children.nonEmpty) || (expandCfg.autoExpand && nodeChildren.hasNext)
  }

  def resultAsString: String = task.resultDisplayString

  override def title: String = task.toPrettyName(NodeFormatUI.useEntityType, NodeFormatUI.abbreviateName)

  override def printSource(): Unit = task.printSource()

  override def equals(other: Any): Boolean = other match {
    case that: NodeTreeView => task == that.task
    case _                  => false
  }

  override def hashCode(): Int = task.hashCode()
  override def toString: String = title // for useful output when copying
}

class NodeTreeViewCompressed(expandCfg: ExpandConfig, level: Int)
    extends NodeTreeView(NodeTreeTable.fakePNodeTask, expandCfg, level) {
  children = new ArrayBuffer[NodeTreeView]()

  override def isCompressed = true
  override def hasChildren = false
  override def title: String = children.length + " nodes"
}

class NodeTreeViewGroup(val group: NodeGroup, level: Int)
    extends NodeTreeView(NodeTreeTable.fakePNodeTask, NodeTreeTable.noAutoExpandConfig, level) {

  override def getChildren: Iterable[NodeTreeView] = {
    if (children eq null) {
      children = new ArrayBuffer[NodeTreeView](group.nodeOrGroupChildCount)
      if (group.groupChildren ne null) {
        val it = group.groupChildren.iterator
        while (it.hasNext) children += new NodeTreeViewGroup(it.next(), level + 1)
      } else if (group.nodeChildrenDirect ne null) {
        val it = group.nodeChildrenDirect.iterator
        while (it.hasNext) children += new NodeTreeView(it.next(), expandCfg, level + 1)
      }
    }
    children
  }
  override def hasChildren: Boolean = group.hasChildren || group.allNodeTasks.nonEmpty
  override def resultAsString: String = ""
  override def title: String = {
    val subGroups = if (group.groupChildren ne null) group.groupChildren.size.toString + " subgroup(s)" else ""
    group.title + "      " + group.start + " node(s) " + subGroups
  }
  override def font: Font = NPTableRenderer.diffFont
  override def icon: Icon = if (open) Icons.folderOpen else Icons.folderClosed
}

class UTrackTreeView(utrack: NodeTask, level: Int)
    extends NodeTreeView(NodeTrace.accessProfile(utrack), NodeTreeTable.noAutoExpandConfig, level) {

  private[ui] def initTweakAndOriginalNodeChildren(nui: NodeToUTrackToInvalidate): Unit = {
    children = new ArrayBuffer[NodeTreeView]()
    if (nui.invalidatingTweak ne null) children += new InvalidatingTweakTreeView(nui.invalidatingTweak, level + 1)
    if (nui.nodes.nonEmpty) {
      val it = nui.nodes.iterator
      while (it.hasNext)
        children += new NodeTreeView(it.next(), NodeTreeTable.noAutoExpandConfig, level + 1)
    }
  }

  override def hasChildren: Boolean = (children ne null) && children.nonEmpty
  override def resultAsString: String = task.resultAsString()
}

class InvalidatingTweakTreeView(twk: Tweak, level: Int)
    extends NodeTreeView(
      twk.target.asInstanceOf[InstancePropertyTarget[_, _]].key.asInstanceOf[NodeTask],
      NodeTreeTable.noAutoExpandConfig,
      level) {

  open = true
  override def hasChildren: Boolean = (children ne null) && children.nonEmpty
  override def icon: Icon = Icons.invalidatingTweak
  override def resultAsString: String = twk.tweakTemplate.toString
  override def printSource(): Unit = println(twk.toString) // revisit this for clickable link
  override def title: String =
    if (twk eq Generic.emptyTweak) "[Not caused by invalidation but by new UI binding]" else super.title
}
