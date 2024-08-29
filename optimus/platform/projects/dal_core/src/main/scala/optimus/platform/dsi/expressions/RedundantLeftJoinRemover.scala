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
package optimus.platform.dsi.expressions

import scala.collection.mutable

class RedundantLeftJoinRemover private () extends ExpressionVisitor {
  private val idToProperties = new mutable.HashMap[Id, mutable.HashSet[Property]]
  private val propertyMappings = new mutable.HashMap[Property, Property]

  def remove(e: Expression): Expression = {
    val result = visit(e)
    if (propertyMappings.isEmpty) result
    else {
      val (searchFor, replaceWith) = propertyMappings.toList.unzip
      ExpressionReplacer.replace(result, searchFor, replaceWith)
    }
  }

  protected override def visitSelect(s: Select): Expression = {
    val where = visitOption(s.where)
    val sortBy = visitList(s.sortBy, visitSortByDef)
    val groupBy = visitExpressionList(s.groupBy)
    val skip = visitOption(s.skip)
    val take = visitOption(s.take)
    val props = visitList(s.properties, visitPropertyDef)
    val from = visit(s.from).asInstanceOf[QuerySource]
    updateSelect(s, from, props, where, sortBy, groupBy, skip, take)
  }

  protected override def visitJoin(j: Join): Expression = {
    (j.on.map(visit), visit(j.right)) match {
      case (Some(Function(EntityOps.Equals.Name, List(p1: Property, p2: Property))), e: Entity)
          if j.joinType == JoinType.LeftOuter &&
            idToProperties(e.id).size == 1 &&
            idToProperties(e.id).head.names == Seq(PropertyLabels.EntityRef) &&
            (p1.owner == e.id || p2.owner == e.id) =>
        if (p1.owner == e.id) propertyMappings.put(p1, p2) else propertyMappings.put(p2, p1)
        visit(j.left)
      case (condition, right: QuerySource) =>
        val left = visit(j.left).asInstanceOf[QuerySource]
        updateJoin(j, left, right, condition)
      case o => throw new MatchError(o)
    }
  }

  protected override def visitProperty(p: Property): Expression = {
    val propSet = idToProperties.getOrElseUpdate(p.owner, new mutable.HashSet[Property]())
    propSet.add(p)
    p
  }
}

object RedundantLeftJoinRemover {
  def remove(e: Expression): Expression = {
    new RedundantLeftJoinRemover().remove(e)
  }
}
