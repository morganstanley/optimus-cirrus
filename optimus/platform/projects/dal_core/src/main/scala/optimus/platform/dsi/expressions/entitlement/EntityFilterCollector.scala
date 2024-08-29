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

import scala.collection.mutable

private class EntityFilterCollector(entities: Map[Id, Entity]) extends ExpressionVisitor {
  import EntityFilterCollector._

  private val filters = new mutable.HashMap[Id, mutable.ListBuffer[Expression]]

  def getEntityFilters(): Map[Id, List[Expression]] = {
    filters.iterator.map { case (id, list) =>
      (id, list.result())
    }.toMap
  }

  protected override def visitSelect(s: Select): Expression = {
    s.where.foreach { w =>
      val sourceIds = PropertyOwnerIdCollector.collect(w, entities)
      sourceIds.foreach(id => addFilter(id, w))
    }
    super.visitSelect(s)
  }

  private def addFilter(id: Id, filter: Expression) = {
    val list = filters.getOrElseUpdate(id, new mutable.ListBuffer[Expression]())
    list.append(filter)
  }
}

private[entitlement] object EntityFilterCollector {
  def collect(entities: Map[Id, Entity], ex: Expression): Map[Id, List[Expression]] = {
    val e = new PropertyRewriter().visit(ex)
    val c = new EntityFilterCollector(entities)
    c.visit(e)
    c.getEntityFilters()
  }

  private class PropertyRewriter extends ExpressionVisitor {
    private[this] var currentFrom: Expression = _

    protected override def visitSelect(s: Select): Expression = {
      val savedFrom = currentFrom
      currentFrom = s.from
      val result = super.visitSelect(s)
      currentFrom = savedFrom
      result
    }

    protected override def visitProperty(p: Property): Expression = {
      // if we cannot find source property, replace owner as emptyId
      // this is to prevent collect where in subquery which references outer scope entity
      findEntityProperty(p).getOrElse(p.copy(owner = Id.EmptyId))
    }

    /**
     * when we find the source property, we step into join with constraints:
     *   1. step into both side of inner/cross join 2. step into left side of left join 3. step into right side of right
     *      join
     *
     * Since only in those cases, the filter condition could be moved closer to Entity
     */
    private def findEntityProperty(via: Property): Option[Property] = {
      new InnerJoinEntityPropertyFinder(via).find(currentFrom).collect { case (_: Entity, p) =>
        p
      }
    }
  }

  private class InnerJoinEntityPropertyFinder(via: Property) extends SourcePropertyFinder(via) {
    protected override def visitJoin(j: Join): Expression = {
      if (foundFromSelect) {
        j.joinType match {
          case JoinType.InnerJoin | JoinType.CrossJoin => visit(j.left); visit(j.right)
          case JoinType.LeftOuter                      => visit(j.left)
          case JoinType.RightOuter                     => visit(j.right)
          case _                                       =>
        }
      } else visit(j.left); visit(j.right)
      j
    }
  }

  // Collect the owner of property and the owner should be Entity in the expression.
  private object PropertyOwnerIdCollector {
    def collect(e: Expression, entityIds: Map[Id, Entity]): Set[Id] = {
      val c = new PropertyOwnerIdCollector(entityIds)
      c.visit(e)
      c.ids.result().toSet
    }
  }

  private class PropertyOwnerIdCollector(entityIds: Map[Id, Entity]) extends ExpressionVisitor {
    private val ids = new mutable.ListBuffer[Id]()

    override def visitProperty(p: Property): Expression = {
      if (entityIds.contains(p.owner)) ids.append(p.owner)
      p
    }
  }
}
