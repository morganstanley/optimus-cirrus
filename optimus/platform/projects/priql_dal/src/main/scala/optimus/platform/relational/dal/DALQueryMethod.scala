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
package optimus.platform.relational.dal

import optimus.platform._
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.inmemory.MethodKeyOptimizer
import optimus.platform.relational.inmemory.Executable
import optimus.platform.relational.inmemory.MultiRelationRewriter
import optimus.platform.relational.internal.TxTimeOrdering.TxTimeAscOrdering
import optimus.platform.relational.internal.TxTimeOrdering.TxTimeDescOrdering
import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.MethodElement
import optimus.platform.relational.tree.MultiRelationElement
import optimus.platform.storable.Entity

object DALQueryMethod {
  object Sampling extends QueryMethod

  object SortByTT extends Executable {
    def execute(method: MethodElement, rewriter: MultiRelationRewriter): NodeFunction0[Iterable[Any]] = {
      val ((s: MultiRelationElement) :: ConstValueElement(order: Boolean, _) :: others) = method.methodArgs.map(_.param)
      val ttOrdering = if (order) TxTimeAscOrdering else TxTimeDescOrdering
      val nf = rewriter.rewrite(s).asInstanceOf[NodeFunction0[Iterable[Entity]]]

      asNode { () =>
        {
          val rows = nf()
          rows.toSeq.sorted(ttOrdering)
        }
      }
    }

    override def toString: String = "SortByTT(DAL)"
  }

  object ProjectedViewOnly extends Executable with MethodKeyOptimizer.Optimizable {
    def execute(method: MethodElement, rewriter: MultiRelationRewriter): NodeFunction0[Iterable[Any]] = {
      throw new RelationalUnsupportedException(
        "'projectedViewOnly' should be used against a DAL query and match the 'projected-only' execution constraint.")
    }

    override def toString: String = "ProjectedViewOnly(DAL)"
  }

  object FullTextSearchOnly extends Executable with MethodKeyOptimizer.Optimizable {
    def execute(method: MethodElement, rewriter: MultiRelationRewriter): NodeFunction0[Iterable[Any]] = {
      throw new RelationalUnsupportedException(
        "'fullTextSearchOnly' should be used against a DAL query and match the execution constraint.")
    }

    override def toString: String = "FullTextSearchOnly(DAL)"
  }

  object Reverse extends Executable {
    def execute(method: MethodElement, rewriter: MultiRelationRewriter): NodeFunction0[Iterable[Any]] = {
      val (s: MultiRelationElement) :: _ = method.methodArgs.map(_.param)
      val nf = rewriter.rewrite(s)
      asNode { () =>
        nf().toSeq.reverse
      }
    }

    override def toString: String = "Reverse(DAL)"
  }
}
