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

import optimus.platform.dsi.expressions.BinaryOperator._
import optimus.platform.dsi.expressions._
import optimus.platform.dsi.expressions.PropertyLabels._
import optimus.platform.dsi.expressions.entitlement.ResourceLinkageType.ChildToParent
import optimus.platform.dsi.expressions.entitlement.ResourceLinkageType.ParentToChild

import scala.collection.mutable

/**
 * For OneToMany linkage (like parent.c2p or embeddable collection), the supported pattern has:
 *   - join between parent Entity and Linkage (on parent_ref), Linkage and child Entity (on child_ref)
 *   - join between Entity and Embeddable (on vref), Embeddable and Entity (on prop = eref)
 *
 * The expected patterns are as follows: SELECT XXX FROM Parent AS t0 INNER JOIN ( SELECT t3.dal$entityRef,
 * t2.parent_ref, t2.parent_prop_name FROM Linkage(Parent) AS t2 LEFT OUTER JOIN Child AS t3 ON
 * entity.equals(t3.dal$entityRef, t2.child_ref) ) AS t1 ON (entity.equals(t1.parent_ref, t0.dal$entityRef) AND
 * (t1.parent_prop_name = 'artifacts'))
 *
 * SELECT ( SELECT XXX FROM Linkage(Parent) AS t1 LEFT OUTER JOIN Child AS t2 ON entity.equals(t2.dal$entityRef,
 * t1.child_ref) WHERE (entity.equals(t1.parent_ref, t0.dal$entityRef) AND (t1.parent_prop_name = 'XXX')) ) AS c1 FROM
 * Parent AS t0
 *
 * SELECT convert.toEntity(t0.dal$entityRef) AS dal$entityRef FROM Parent AS t0 WHERE entity.in('$artRef', ( SELECT
 * t2.dal$$entityRef FROM Linkage(Parent) AS t1 LEFT OUTER JOIN Child AS t2 ON entity.equals(t2.dal$entityRef,
 * t1.child_ref) WHERE (entity.equals(t1.parent_ref, t0.dal$entityRef) AND (t1.parent_prop_name = 'artifacts'))
 *
 * SELECT XXX FROM Parent AS t0 INNER JOIN ( SELECT t2.dal$versionRef FROM Parent_items_Embeddable AS t2 LEFT OUTER JOIN
 * Child AS t3 ON entity.equals(t3.dal$entityRef, t2.entityPropOfEmbeddable) ) AS t1 ON (t1.dal$versionRef =
 * t0.dal$versionRef)
 */
private class OneToManyLinkageCollector(expr: Expression) extends ExpressionVisitor {
  private val resourceLinkages = new mutable.HashSet[ResourceLinkage]()
  private val linkageToChildEntity = new mutable.HashMap[Id, Id]()
  private val embeddableToEntity = new mutable.HashMap[Id, mutable.HashSet[(Id, String)]]()
  private val (linkages, entities) = {
    val linkageList = new mutable.ListBuffer[Id]
    val entityList = new mutable.ListBuffer[Id]
    QuerySourceCollector.collect(
      expr,
      {
        case e: Entity  => entityList += e.id
        case e: Linkage => linkageList += e.id
      })
    (linkageList.toSet, entityList.toSet)
  }

  protected override def visitJoin(j: Join): Expression = {
    val join = super.visitJoin(j)

    (j.joinType, j.on) match {
      case (JoinType.LeftOuter | JoinType.InnerJoin, Some(Function(EntityOps.Equals.Name, List(prop1, prop2)))) =>
        (prop1, prop2) match {
          case (
                Property(PropertyType.Special, Seq(ChildRef), linkageId),
                Property(PropertyType.Special, Seq(EntityRef), entityId)) =>
            addLinkageToChildEntity(linkageId, entityId)

          case (
                Property(PropertyType.Special, Seq(EntityRef), entityId),
                Property(PropertyType.Special, Seq(ChildRef), linkageId)) =>
            addLinkageToChildEntity(linkageId, entityId)

          case (Property(PropertyType.Special, Seq(name), entityId), p: Property) if name.startsWith(EntityRef) =>
            addEmbeddableToEntities(j, entityId, p)

          case (p: Property, Property(PropertyType.Special, Seq(name), entityId)) if name.startsWith(EntityRef) =>
            addEmbeddableToEntities(j, entityId, p)

          case _ =>
        }

      case (JoinType.InnerJoin, Some(e)) =>
        // Match Join between Parent and Linkage(Parent).
        collectOneToManyRelationship(e)

      case _ =>
    }

    join
  }

  override def visitSelect(s: Select): Expression = {
    val select = super.visitSelect(s)
    // Join between Parent and Linkage(Parent) could be in select.where.
    s.where.foreach(collectOneToManyRelationship)
    select
  }

  def collectOneToManyRelationship(e: Expression): Unit = {
    e match {
      case Binary(
            AndAlso,
            Function(EntityOps.Equals.Name, List(prop1: Property, prop2: Property)),
            Binary(Equal, p @ Property(_, Seq(ParentPropName), linkageId), Constant(propertyName: String, _))) =>
        // Only add the LinkageRelation when it already has join between Linkage(Parent) and Children.
        SourcePropertyFinder.findLinkageProperty(expr, p).map { l =>
          linkageToChildEntity.get(l._1.id).foreach { childId =>
            (prop1, prop2) match {
              case (
                    Property(PropertyType.Special, Seq(ParentRef), `linkageId`),
                    Property(PropertyType.Special, Seq(EntityRef), parentId)) =>
                addResourceLinkage(parentId, childId, propertyName)

              case (
                    Property(PropertyType.Special, Seq(EntityRef), parentId),
                    Property(PropertyType.Special, Seq(ParentRef), `linkageId`)) =>
                addResourceLinkage(parentId, childId, propertyName)

              case _ =>
            }
          }
        }

      case Binary(Equal, p1 @ Property(_, Seq(VersionedRef), _), p2 @ Property(_, Seq(VersionedRef), _)) =>
        (SourcePropertyFinder.find(expr, p1), SourcePropertyFinder.find(expr, p2)) match {
          case (Some((e1: Entity, _)), Some((e2: Embeddable, _))) =>
            embeddableToEntity
              .get(e2.id)
              .foreach(_.foreach { case (cid, prop) =>
                addResourceLinkage(e1.id, cid, s"${e2.property}.$prop")
              })
          case (Some((e2: Embeddable, _)), Some((e1: Entity, _))) =>
            embeddableToEntity
              .get(e2.id)
              .foreach(_.foreach { case (cid, prop) =>
                addResourceLinkage(e1.id, cid, s"${e2.property}.$prop")
              })
          case _ =>
        }

      case Binary(AndAlso, left, right) =>
        collectOneToManyRelationship(left)
        collectOneToManyRelationship(right)

      case _ =>
    }
  }

  def addEmbeddableToEntities(j: Join, entityId: Id, via: Property) = {
    SourcePropertyFinder.findEmbeddableProperty(j.left, via).foreach { case (embeddable, p) =>
      // We need make sure there is only one Entity in right and it directly joins j.left.
      val rightEntityIds = QuerySourceCollector.collectEntities(j.right).map(_.id)
      if (rightEntityIds.length == 1 && rightEntityIds.head == entityId) {
        val value = embeddableToEntity.getOrElseUpdate(embeddable.id, new mutable.HashSet[(Id, String)])
        value.add((entityId, p.names.head))
      }
    }
  }

  private def addLinkageToChildEntity(linkageId: Id, entityId: Id) = {
    if (linkages.contains(linkageId) && entities.contains(entityId) && !linkageToChildEntity.contains(linkageId))
      linkageToChildEntity.put(linkageId, entityId)
  }

  private def addResourceLinkage(parentId: Id, childId: Id, propertyName: String) = {
    if (entities.contains(parentId)) {
      resourceLinkages += ResourceLinkage(parentId, childId, propertyName, ParentToChild)
      resourceLinkages += ResourceLinkage(childId, parentId, propertyName, ChildToParent)
    }
  }
}

private[entitlement] object OneToManyLinkageCollector {
  def collect(expr: Expression) = {
    val collector = new OneToManyLinkageCollector(expr)
    collector.visit(expr)
    collector.resourceLinkages.toSet
  }
}
