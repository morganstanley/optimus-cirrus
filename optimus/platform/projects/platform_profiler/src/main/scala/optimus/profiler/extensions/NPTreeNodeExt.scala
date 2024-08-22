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
package optimus.profiler.extensions

import java.awt.Color

import optimus.graph.diagnostics.SelectionFlags
import optimus.graph.diagnostics.NPTreeNode
import optimus.profiler.ui.NPTableRenderer
import optimus.profiler.ui.TableColumn
import optimus.profiler.ui.TreeSelector

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object NPTreeNodeExt {

  @tailrec def openToFirst[RType <: NPTreeNode](grp: RType, openToPredicate: RType => Boolean): Unit = {
    grp.open = true
    if (grp.hasChildren) {
      val it = grp.getChildren.iterator
      if (it.hasNext) {
        val child = it.next().asInstanceOf[RType]
        if (!openToPredicate(child))
          openToFirst(child, openToPredicate)
        else
          child.diffFlag = SelectionFlags.Highlighted
      }
    } else
      grp.diffFlag = SelectionFlags.Incomplete
  }

  def getRowBackground[T <: NPTreeNode](col: TableColumn[T], row: T): Color = {
    val flag = row.diffFlag
    if (flag eq SelectionFlags.Highlighted) Color.pink
    else if (flag eq SelectionFlags.Incomplete) NPTableRenderer.lightPink
    else null
  }

  def expandTree[RType <: NPTreeNode](seq: Iterable[NPTreeNode], forceOpen: Boolean = false): ArrayBuffer[RType] = {
    val lst = new ArrayBuffer[RType]()

    def expand(seq: Iterable[NPTreeNode]): Unit = {
      val it = seq.iterator
      while (it.hasNext) {
        val ssu = it.next()
        if (ssu ne null) {
          lst += ssu.asInstanceOf[RType]
          if (forceOpen) ssu.open = true
          if (ssu.open && ssu.hasChildren)
            expand(ssu.getChildren)
        }
      }
    }

    expand(seq)
    lst
  }

  /** Expand the approximately first maxCount nodes/children in the BFS */
  def openTreeBFS[RType <: NPTreeNode](seq: Iterable[NPTreeNode], maxCount: Int): Unit = {
    var stack = new ArrayBuffer[NPTreeNode]()
    var itemCount = seq.size
    stack ++= seq

    def open(seq: Iterable[NPTreeNode]): Unit = {
      for (ssu <- seq if (ssu ne null) && itemCount < maxCount) {
        ssu.open = true
        if (ssu.hasChildren) {
          val children = ssu.getChildren
          itemCount += children.size
          stack ++= ssu.getChildren
        }
      }
    }

    while (itemCount < maxCount && stack.nonEmpty) {
      val lastStack = stack
      stack = new ArrayBuffer[NPTreeNode]()
      open(lastStack)
    }
  }

  /** DFS traversal */
  def openTo[RType <: NPTreeNode](seq: Iterable[NPTreeNode], stopOnFirst: Boolean, pred: RType => Boolean): Boolean = {
    var open = false
    for (i <- seq if i ne null) {
      val ti = i.asInstanceOf[RType]
      if (pred(ti)) open = true

      if (!(stopOnFirst && open)) {
        val cseq =
          ti.getChildren // can (apparently) be null in NodeStacks table because awaited end of chain task may no longer be available
        ti.open = if (cseq != null && cseq.nonEmpty) openTo(cseq, stopOnFirst, pred) else false
        open |= ti.open
      }
    }
    open
  }

  /**
   * DFS traversal Returns true if current level needs to be open. i.e. because the node of interest is below
   */
  def openTo[RType <: NPTreeNode](seq: Iterable[NPTreeNode], pred: TreeSelector[RType], compress: Boolean): Boolean = {
    var open = false
    val it = seq.iterator
    while (it.hasNext) {
      val i = it.next().asInstanceOf[RType]
      if (i ne null) {
        pred.review(i)
        if (pred.matched) {
          open = true
          i.open = false
          i.matched = true
        } else {
          i.matched = false
          if (pred.maybeLower && i.hasChildren) {
            i.open = openTo(i.getUncompressedChildren, pred.moveDown(), compress)
            if (i.open && compress) i.compressClosed()
            open |= i.open
          } else i.open = false
        }
      }
    }
    open
  }
}
