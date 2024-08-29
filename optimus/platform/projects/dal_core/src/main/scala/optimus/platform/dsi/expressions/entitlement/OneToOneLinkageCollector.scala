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
import optimus.platform.dsi.expressions.entitlement.ResourceLinkageType.ChildToParent
import optimus.platform.dsi.expressions.entitlement.ResourceLinkageType.ParentToChild

import scala.collection.mutable

/**
 * For OneToOne linkage relation (like trade.position), we only support the direct join between Trade and Position. The
 * expected pattern as follows: """SELECT XXX FROM Trade AS t0 LEFT OUTER JOIN Position AS t1 ON
 * entity.equals(t1.dal$entityRef, t0.position)"""
 */
private class OneToOneLinkageCollector extends ExpressionVisitor {
  import OneToOneLinkageCollector._
  import SourcePropertyFinder._

  private val resourceLinkages = new mutable.HashSet[ResourceLinkage]()

  protected override def visitJoin(j: Join): Expression = {
    (j.joinType, j.on) match {
      case (JoinType.LeftOuter, Some(EntityRefPropertyEqualsToViaProperty(ref, via))) =>
        collectResourceLinkages(j, ref.owner, via)
      case (JoinType.InnerJoin, Some(EntityRefPropertyEqualsToViaProperty(ref, via))) =>
        (findEntityProperty(j, via), findEntityProperty(j, ref)) match {
          case (Some((e1, p1)), Some((e2, _))) =>
            addResourceLinkage(e1.id, e2.id, p1.names.head)
          case _ =>
        }
      case _ =>
    }
    super.visitJoin(j)
  }

  private def collectResourceLinkages(j: Join, entityRefOwner: Id, via: Property) = {
    findEntityProperty(j.left, via).foreach { case (leftEntity, p) =>
      // We need make sure there is only one Entity in right and it directly joins j.left.
      val rightEntityIds = QuerySourceCollector.collectEntities(j.right).map(_.id)
      if (rightEntityIds.length == 1 && rightEntityIds.head == entityRefOwner) {
        addResourceLinkage(leftEntity.id, entityRefOwner, p.names.head)
      }
    }
  }

  // Add the LinkageRelation (left.right)
  private def addResourceLinkage(leftId: Id, rightId: Id, propertyName: String) = {
    resourceLinkages += ResourceLinkage(leftId, rightId, propertyName, ParentToChild)
    resourceLinkages += ResourceLinkage(rightId, leftId, propertyName, ChildToParent)
  }
}

private[entitlement] object OneToOneLinkageCollector {
  def collect(ex: Expression) = {
    val collector = new OneToOneLinkageCollector
    collector.visit(ex)
    collector.resourceLinkages.toSet
  }

  object EntityRefPropertyEqualsToViaProperty {
    def unapply(e: Expression): Option[(Property, Property)] = {
      e match {
        case Function(EntityOps.Equals.Name, List(p @ Property(PropertyType.Special, Seq(name), _), via: Property))
            if name.startsWith(EntityRef) =>
          Some(p -> via)
        case Function(EntityOps.Equals.Name, List(via: Property, p @ Property(PropertyType.Special, Seq(name), _)))
            if name.startsWith(EntityRef) =>
          Some(p -> via)
        case _ => None
      }
    }
  }
}
