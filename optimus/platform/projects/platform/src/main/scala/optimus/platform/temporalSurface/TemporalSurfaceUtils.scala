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
package optimus.platform.temporalSurface

import java.time.Instant

import optimus.platform.TemporalContext
import optimus.platform.temporalSurface.impl.LeafTemporalSurfaceImpl

object TemporalSurfaceUtils {
  import PrivateImpl._

  /**
   * walk a tree and generate another optional tree, while performing arbitrary surgery on the tree
   * @param temporalSurface
   *   the temporal surface or context to walk
   * @param walker
   *   the tree walker for the supplied surface
   * @return
   *   the result of the tree walk
   */
  def treeWalk(temporalSurface: TemporalSurface, walker: TSMapper): List[TemporalSurface] = map(temporalSurface, walker)

  trait TSMapper {
    def shouldStartBranch(branch: BranchTemporalSurface, path: List[TemporalSurface]): Boolean
    def endBranch(
        branch: BranchTemporalSurface,
        childrenWereMapped: Boolean,
        childrenWereChanged: Boolean,
        mappedChildren: List[TemporalSurface],
        path: List[TemporalSurface]): List[TemporalSurface]
    def mapLeaf(leaf: LeafTemporalSurface, path: List[TemporalSurface]): List[TemporalSurface]
    protected def currentTemporality(leaf: LeafTemporalSurface) = leaf match {
      case tsi: LeafTemporalSurfaceImpl => tsi.currentTemporality
    }
  }

  def treeWalk(tc: TemporalContext, walker: TSMapper): List[TemporalSurface] =
    TemporalSurfaceUtils.treeWalk(tc.asInstanceOf[TemporalSurface], walker)

  def asContext(surfaces: List[TemporalSurface]): TemporalContext = surfaces.distinct match {
    case List(ret: TemporalContext) => ret
    case x                          => throw new IllegalStateException(s"code error $x") // this is a code error
  }

  final class ToFixedTransactionTime(tt: Instant) extends TemporalSurfaceUtils.TSMapper {
    import TemporalSurfaceDefinition._

    override def shouldStartBranch(branch: BranchTemporalSurface, path: List[TemporalSurface]): Boolean = true
    override def endBranch(
        branch: BranchTemporalSurface,
        childrenWereMapped: Boolean,
        childrenWereChanged: Boolean,
        mappedChildren: List[TemporalSurface],
        path: List[TemporalSurface]): List[TemporalSurface] =
      if (childrenWereChanged) {
        val newContext =
          if (path.isEmpty) BranchContext(branch.scope, mappedChildren, branch.tag)
          else
            BranchSurface(branch.scope, mappedChildren, branch.tag)
        newContext :: Nil
      } else branch :: Nil

    override def mapLeaf(leaf: LeafTemporalSurface, path: List[TemporalSurface]): List[TemporalSurface] = {
      val current = currentTemporality(leaf)
      if (current.txTime != tt)
        FixedLeaf(leaf.acceptDelegation, leaf.matcher, current.validTime, tt, leaf.tag) :: Nil
      else leaf :: Nil
    }
  }

  class TransformValidTime(vtTransformer: Instant => Instant) extends TemporalSurfaceUtils.TSMapper {
    import TemporalSurfaceDefinition._

    override def shouldStartBranch(branch: BranchTemporalSurface, path: List[TemporalSurface]): Boolean = true
    override def endBranch(
        branch: BranchTemporalSurface,
        childrenWereMapped: Boolean,
        childrenWereChanged: Boolean,
        mappedChildren: List[TemporalSurface],
        path: List[TemporalSurface]): List[TemporalSurface] =
      if (childrenWereChanged) {
        val newContext =
          if (path.isEmpty) BranchContext(branch.scope, mappedChildren, branch.tag)
          else
            BranchSurface(branch.scope, mappedChildren, branch.tag)
        newContext :: Nil
      } else branch :: Nil

    override def mapLeaf(leaf: LeafTemporalSurface, path: List[TemporalSurface]): List[TemporalSurface] = {
      val current = currentTemporality(leaf)
      val newVT = vtTransformer(current.validTime)
      if (current.validTime != newVT)
        FixedLeaf(leaf.acceptDelegation, leaf.matcher, newVT, current.txTime, leaf.tag) :: Nil
      else leaf :: Nil
    }
  }

  // keep all of the implementation private
  private object PrivateImpl {
    def CANT_HAPPEN(msg: String): Nothing = throw new IllegalStateException(s"code error : $msg")
    def map(ts: TemporalSurface, mapper: TSMapper): List[TemporalSurface] = {
      def mapImpl(ts: TemporalSurface, path: List[TemporalSurface]): List[TemporalSurface] = {
        ts match {
          case leaf: LeafTemporalSurface => mapper.mapLeaf(leaf, path)
          case branch: BranchTemporalSurface =>
            val mapChildren = mapper.shouldStartBranch(branch, path)
            val (mappedChildren, childrenChanged) = if (mapChildren) {
              val newPath = branch :: path
              val children = branch.children.flatMap { c =>
                mapImpl(c, newPath)
              }
              val equal = children == branch.children
              if (equal) (branch.children, false) else (children, true)
            } else (branch.children, false)
            mapper.endBranch(branch, mapChildren, childrenChanged, mappedChildren, path)
          case x => CANT_HAPPEN(x.getClass.getName)
        }
      }
      mapImpl(ts, Nil)
    }
  }
}
