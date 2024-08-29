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
package optimus.platform.dsi.expressions.entitlement

import optimus.platform.dsi.expressions._
import optimus.platform.dsi.expressions.PropertyLabels.EntityRef

import scala.collection.mutable

private class EqualEntityGroupCollector(expr: Expression) extends ExpressionVisitor {
  private val equalEntityGroup = new mutable.ListBuffer[mutable.HashSet[Id]]()

  protected override def visitJoin(j: Join): Expression = {
    // If two entity nodes are inner join on their dal$entityref and have the same classname, they are equal.
    if (j.joinType == JoinType.InnerJoin) {
      j.on.foreach { matchEntityRefEqual }
    }

    super.visitJoin(j)
  }

  private def matchEntityRefEqual(e: Expression): Unit = {
    e match {
      case Binary(BinaryOperator.AndAlso, left, right) =>
        matchEntityRefEqual(left)
        matchEntityRefEqual(right)

      case Function(
            EntityOps.Equals.Name,
            List(
              p1 @ Property(PropertyType.Special, Seq(name1), _),
              p2 @ Property(PropertyType.Special, Seq(name2), _)))
          if name1.startsWith(EntityRef) && name2.startsWith(EntityRef) =>
        collectEqualEntity(p1, p2)

      case _ =>
    }
  }

  private def collectEqualEntity(p1: Property, p2: Property) = {
    SourcePropertyFinder.findEntityProperty(expr, p1).foreach { case (e1, _) =>
      val entity2Opt = SourcePropertyFinder.findEntityProperty(expr, p2)
      entity2Opt.foreach { case (e2, _) =>
        // Check if the two source entities are equal
        if (e1.name == e2.name && e1.when == e2.when) {
          val group =
            equalEntityGroup.find(g => g.contains(e1.id) || g.contains(e2.id)).getOrElse {
              val empty = new mutable.HashSet[Id]()
              equalEntityGroup.append(empty)
              empty
            }
          group += e1.id
          group += e2.id
        }
      }
    }
  }
}

// Collect the equal entity nodes that are inner join and have the same classname.
private[entitlement] object EqualEntityGroupCollector {
  def collect(expr: Expression): EqualEntityGroup = {
    val collector = new EqualEntityGroupCollector(expr)
    collector.visit(expr)
    val targetToIds = collector.equalEntityGroup.map(g => g.head -> g.tail.toSet).toMap
    val idToTarget = targetToIds.flatMap { case (id, entityGroup) => entityGroup.map(_ -> id) }
    EqualEntityGroup(targetToIds, idToTarget)
  }
}

final case class EqualEntityGroup(targetToIds: Map[Id, Set[Id]], idtoTarget: Map[Id, Id]) {
  def isEmpty(): Boolean = targetToIds.isEmpty
}
