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
package optimus.profiler.ui.common

import javax.swing.JMenuItem
import optimus.core.CoreHelpers
import optimus.graph.NodeTaskInfo
import optimus.graph.PropertyNode
import optimus.graph.SSCacheID
import optimus.graph.cache.NCPolicy
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.Scenario
import optimus.profiler.DebuggerUI

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GroupingAttribute extends Enumeration {
  type GroupingAttribute = ValueEx
  class ValueEx(val groupTemplate: NodeGroup) extends Val(groupTemplate.name)

  val Arguments: GroupingAttribute = new ValueEx(new NodeGroupArgs())
  val ArgumentsDisplayed: GroupingAttribute = new ValueEx(new NodeGroupArgsDisplayed())
  val NodeNameAttr: GroupingAttribute = new ValueEx(new NodeGroupNodeName())
  val Class: GroupingAttribute = new ValueEx(new NodeGroupClass())
  val AppletName: GroupingAttribute = new ValueEx(new NodeGroupAppletName())
  val Package: GroupingAttribute = new ValueEx(new NodeGroupPackage())
  val ScenarioStack: GroupingAttribute = new ValueEx(new NodeGroupScenarioStack())
  val Result: GroupingAttribute = new ValueEx(new NodeGroupTaskResult())
  val Process: GroupingAttribute = new ValueEx(new NodeGroupProcess())
}

class AllInputs(val node: PropertyNode[_], val equivScenario: Scenario) {
  override def equals(obj: Any): Boolean = obj match {
    case b: AllInputs => node.equalsForCaching(b.node) && equivScenario.equals(b.equivScenario)
    case _            => false
  }
  override def hashCode: Int = node.hashCodeForCaching * 41 + equivScenario.hashCode
  override def toString: String = s"Inputs @$hashCode"
}

abstract class NodeGroup(override val level: Int, val key: AnyRef) extends NPTreeNode {
  open = true
  var start: Int = _
  var computeTime: Long = _
  final def saveTime: Long = if (start == 0) 0L else (computeTime * (start - 1)) / start

  private var childrenMap: mutable.Map[AnyRef, NodeGroup] = _ // While grouping...
  protected var children: ArrayBuffer[NodeGroup] = _ // After trim()
  private var nodeTasks: ArrayBuffer[PNodeTask] = _ // Leaf group will just keep all the nodes inserted
  def allNodeTasks: ArrayBuffer[PNodeTask] = if (nodeTasks eq null) ArrayBuffer.empty[PNodeTask] else nodeTasks

  def getAllNodeTasksOfChildren: ArrayBuffer[PNodeTask] = {
    def getNodeTasksOfChildren(nodeGroup: NodeGroup): ArrayBuffer[PNodeTask] = {
      val nodeGroupChildren = nodeGroup.children
      if ((nodeGroupChildren ne null) && nodeGroupChildren.nonEmpty)
        nodeGroupChildren.flatMap(child => getNodeTasksOfChildren(child))
      else
        nodeGroup.allNodeTasks
    }

    getNodeTasksOfChildren(nodeGroup = this)
  }

  override def title: String = title(key)
  override def getChildren: Iterable[NPTreeNode] = children
  override def hasChildren: Boolean = children ne null

  val toolTip: String = ""

  final def nodeOrGroupChildCount: Int = if (nodeTasks ne null) nodeTasks.size else children.size
  final def groupChildren: ArrayBuffer[NodeGroup] = children
  final def nodeChildrenDirect: ArrayBuffer[PNodeTask] = nodeTasks

  /** for producing menu options */
  def name: String

  def notInterestedIfContainsSingleLeaf: Boolean = false // If non-unique result

  @tailrec
  final def insert(task: PNodeTask, protoTypes: List[NodeGroup]): Unit = {
    start += 1
    if (protoTypes.nonEmpty) {
      if (childrenMap eq null) childrenMap = mutable.Map.empty[AnyRef, NodeGroup]
      val head = protoTypes.head
      val key = head.getKey(task)
      val subGrp = childrenMap.getOrElse(
        key, {
          val newGrp = head.create(level + 1, key)
          childrenMap.put(key, newGrp)
          newGrp
        })

      computeTime += task.getSelfPlusANCTime
      subGrp.insert(task, protoTypes.tail)
    } else {
      if (nodeTasks eq null) nodeTasks = new ArrayBuffer[PNodeTask]()
      nodeTasks += task
    }
  }

  final def trimSimple(): Unit = {
    if (childrenMap ne null) {
      children = new ArrayBuffer[NodeGroup](childrenMap.size)
      val it = childrenMap.iterator
      while (it.hasNext) {
        val (_, grp) = it.next()
        grp.trimSimple()
        children += grp
      }
      childrenMap = null
    }
  }

  /** Returns true if the branch needs to be removed */
  final def trimAndSort(): Boolean = {
    if (notInterestedIfContainsSingleLeaf && start <= 1)
      true
    else if (childrenMap eq null)
      false // Leaf Node Group
    else {
      children = new ArrayBuffer[NodeGroup]()
      val it = childrenMap.iterator
      while (it.hasNext) {
        val (key, grp) = it.next()
        if (grp.trimAndSort()) {
          // Remove group
          childrenMap -= key
          start -= grp.start
          computeTime -= grp.computeTime
        } else
          children += grp
      }
      childrenMap = null
      if (children.isEmpty) true
      else {
        children = children.sortBy(-_.saveTime)
        false
      }
    }
  }

  def getKey(task: PNodeTask): AnyRef
  def create(level: Int, key: AnyRef): NodeGroup
  def title(key: AnyRef): String = CoreHelpers.safeToString(key)

  def modifyMenus(miGroup: JMenuItem, miIndividual: JMenuItem): Boolean = {
    miGroup.setVisible(false)
    miIndividual.setVisible(false)
    false
  }
  def doAction(): Unit = {}
}

class NodeGroupTotals extends NodeGroup(0, "Totals") {
  override def getKey(task: PNodeTask): AnyRef = throw new IllegalArgumentException
  override def create(level: Int, key: AnyRef): NodeGroup = throw new IllegalArgumentException
  override def name: String = "Totals"
}

class NodeGroupCacheType(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = task.propertyCachePolicy
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupCacheType(level, key)
  override def name: String = "Cache Type"
  override def modifyMenus(miGroup: JMenuItem, miIndividual: JMenuItem): Boolean = {
    if (key == NCPolicy.DEFAULT_POLICY_NAME) {
      miGroup.setVisible(true)
      miIndividual.setVisible(false)
      true
    } else
      super.modifyMenus(miGroup, miIndividual)
  }
  override def doAction(): Unit = {
    children.foreach { ng =>
      ng.key match {
        case nti: NodeTaskInfo if !nti.isTweakable => nti.markAsProfilerUIConfigured()
        case _                                     =>
      }
    }
  }
}

class NodeGroupTaskInfo(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = task.nodeName()
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupTaskInfo(level, key)
  override def title(key: AnyRef): String = key.asInstanceOf[NodeName].toString(true)
  override def name: String = "Property Name"
}

class NodeGroupProcess(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = task.process
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupProcess(level, key)
  override def title(key: AnyRef): String = key.toString
  override def name: String = "Process"
}

class NodeGroupTaskResult(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = {
    val res = task.resultKey
    val currConverter = DebuggerUI.currResultLambda

    if (currConverter ne null) currConverter(res) else res
  }
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupTaskResult(level, key)
  override def notInterestedIfContainsSingleLeaf: Boolean = true
  override def name: String = "Result"
}

class NodeGroupArgs(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = {
    val args = task.args
    if (args eq null) "[No Args]" else args.toSeq
  }
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupArgs(level, key)
  override def name: String = "Arguments"
  override def title(key: AnyRef): String = key match {
    case s: Seq[_] => s.size.toString + " Arg(s) @" + s.hashCode().toHexString
    case _         => key.toString
  }
}

class NodeGroupArgsDisplayed(level: Int = 0, key: AnyRef = null) extends NodeGroupArgs(level, key) {
  override def name: String = "Arguments Displayed"
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupArgsDisplayed(level, key)
  override def title(key: AnyRef): String = key match {
    case s: Seq[_] => s.mkString("(", ",", ")")
    case _         => key.toString
  }
}

class NodeGroupScenarioStack(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = task.scenarioStackCacheID
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupScenarioStack(level, key)
  override def name: String = "Scenario Stack"
  override def title(key: AnyRef): String = "SS_" + key.asInstanceOf[SSCacheID].id
}

class NodeGroupNodeName(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = task.nodeName()
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupNodeName(level, key)
  override def name: String = "Node Name"
}

class NodeGroupClass(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = task.nodeClass
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupClass(level, key)
  override def name: String = "Class"
  override def title(key: AnyRef): String = NodeName.cleanNodeClassName(key.asInstanceOf[Class[_]])
}

class NodeGroupPackage(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = task.nodeName().pkgName
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupPackage(level, key)
  override def name: String = "Package"
}

class NodeGroupAppletName(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = {
    val scenarioStack = task.scenarioStack()
    if (scenarioStack ne null) scenarioStack.tweakableListener.dependencyTrackerRootName else "[not tracking]"
  }
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupPackage(level, key)
  override def name: String = "Applet Name"
  override val toolTip: String = "This option is only for UI app with multiple applets."
}

class NodeGroupEntity(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def getKey(task: PNodeTask): AnyRef = {
    val ntsk = task.getTask
    if (ntsk != null) ntsk.asInstanceOf[PropertyNode[_]].entity else "Not available"
  }
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupEntity(level, key)
  override def name: String = "Entity"
}

class NodeGroupAllInputs(level: Int = 0, key: AnyRef = null) extends NodeGroup(level, key) {
  override def notInterestedIfContainsSingleLeaf: Boolean = true
  override def getKey(task: PNodeTask): AnyRef = {
    val ntsk = task.getTask
    if (ntsk != null) new AllInputs(ntsk.asInstanceOf[PropertyNode[_]], DebuggerUI.minimumScenarioOf(ntsk))
    else "Not available"
  }
  override def create(level: Int, key: AnyRef): NodeGroup = new NodeGroupAllInputs(level, key)
  override def name: String = "All Inputs"
}
