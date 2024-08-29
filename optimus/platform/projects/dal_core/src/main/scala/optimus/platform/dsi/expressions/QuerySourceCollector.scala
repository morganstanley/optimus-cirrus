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

class QuerySourceCollector extends ExpressionVisitor {
  private val sources = new mutable.ListBuffer[QuerySource]

  protected override def visitEntity(e: Entity): Expression = {
    sources.append(e)
    e
  }

  protected override def visitEntityBitemporalSpace(e: EntityBitemporalSpace): Expression = {
    sources.append(e)
    e
  }

  protected override def visitLinkage(e: Linkage): Expression = {
    sources.append(e)
    e
  }

  protected override def visitEmbeddable(e: Embeddable): Expression = {
    sources.append(e)
    e
  }

  protected override def visitEvent(e: Event): Expression = {
    sources.append(e)
    e
  }
}

object QuerySourceCollector {
  def collectEntities(ex: Expression): List[Entity] = {
    collect(ex, { case e: Entity => e })
  }

  def collectEntitySpaces(ex: Expression): List[EntityBitemporalSpace] = {
    collect(ex, { case e: EntityBitemporalSpace => e })
  }

  def collectLinkages(ex: Expression): List[Linkage] = {
    collect(ex, { case e: Linkage => e })
  }

  def collect[T](ex: Expression, pf: PartialFunction[QuerySource, T]): List[T] = {
    val c = new QuerySourceCollector
    c.visit(ex)
    c.sources.iterator.collect(pf).toList
  }
}

class PropertyCollector(owners: Set[Id]) extends ExpressionVisitor {
  private val properties: Map[Id, mutable.ListBuffer[Property]] =
    owners.iterator.map(i => i -> new mutable.ListBuffer[Property]).toMap

  override def visitProperty(p: Property): Expression = {
    properties.get(p.owner).foreach(l => l += p)
    p
  }

  def getProperties: Map[Id, List[Property]] =
    properties.iterator.map { case (id, props) => (id, props.distinct.toList) }.toMap
}

object PropertyCollector {
  def collect(e: Expression): Map[Id, List[Property]] = {
    val ids = QuerySourceCollector
      .collect(
        e,
        {
          case e: Entity                => e.id
          case e: EntityBitemporalSpace => e.id
          case e: Event                 => e.id
          case e: Linkage               => e.id
          case e: Embeddable            => e.id
        })
      .toSet
    val collector = new PropertyCollector(ids)
    collector.visit(e)
    collector.getProperties
  }
}
