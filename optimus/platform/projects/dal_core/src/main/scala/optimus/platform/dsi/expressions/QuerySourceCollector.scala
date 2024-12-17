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

import optimus.entity.EntityInfoRegistry
import optimus.entity.IndexInfo
import optimus.platform._
import optimus.platform.dsi.expressions.PropertyType.Index
import optimus.platform.dsi.expressions.PropertyType.Key
import optimus.platform.dsi.expressions.PropertyType.UniqueIndex
import optimus.platform.storable.Storable
import optimus.platform.util.Log

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

private[optimus] object QueryClassPathValidator extends Log {
  def validate(e: Expression): Unit = {
    val entityName = e.asInstanceOf[Select].from.asInstanceOf[Entity].name
    val indexesFromEntity = getIndexesFromClassPath(entityName)
    val indexesFromExpression = getIndexesFromExpression(e, entityName)
    assertAllIndexesFromExpressionExistsInClasspath(indexesFromExpression, indexesFromEntity, entityName)
  }

  def getIndexesFromClassPath(entityName: String): Seq[IndexInfo[_ <: Storable, _]] = {
    val entityInfo = EntityInfoRegistry.getClassInfo(entityName)
    log.info(s"All indexes from entity {$entityName}: {${entityInfo.indexes.map(_.name)}}")
    entityInfo.indexes
  }

  def getIndexesFromExpression(e: Expression, entityName: String): Iterable[Property] = {
    val properties = PropertyCollector
      .collect(e)
      .values
      .flatten
      .filter(p => p.propType == Index || p.propType == Key || p.propType == UniqueIndex)
    log.info(s"All indexes from expression for entity {$entityName}: {${properties.flatMap(_.names)}")
    properties
  }

  def assertAllIndexesFromExpressionExistsInClasspath(
      indexesFromExpression: Iterable[Property],
      indexesFromEntity: Seq[IndexInfo[_ <: Storable, _]],
      entityName: String): Unit = {
    val indexNames = indexesFromEntity.map(_.name)
    indexesFromExpression.foreach(indexExpr => {
      require(
        indexNames.contains(indexExpr.names.head),
        s"expression query field {${indexExpr.names}} is not found in indexes/keys of entity {$entityName}, please check out entity schema!"
      )
      val indexInfoMappings = indexesFromEntity.filter(_.name == indexExpr.names.head)
      require(indexInfoMappings.nonEmpty, s"Index from expression are not found in entity schema!")
      require(indexInfoMappings.size == 1, s"Index from expression are found more than one field in entity schema!")
      val indexInfo = indexInfoMappings.head
      indexExpr.propType match {
        case UniqueIndex =>
          require(
            indexInfo.unique && indexInfo.indexed,
            s"unique index field {${indexExpr.names}} from expression query is not unique index of entity {$entityName}, please check out entity schema!"
          )
        case Key =>
          require(
            indexInfo.unique && !indexInfo.indexed,
            s"key field {${indexExpr.names}} from expression query is not key of entity {$entityName}, please check out entity schema!"
          )
        case Index =>
          require(
            !indexInfo.unique && indexInfo.indexed,
            s"index field {${indexExpr.names}} from expression query is not normal index of entity {$entityName}, please check out entity schema!"
          )
        case _ =>
          log.error(s"Indexes from expression have property type: ${indexExpr.propType}, please checkout expression.")
          throw new IllegalArgumentException(
            s"Indexes from expression have property type: ${indexExpr.propType}, please checkout expression.")
      }
    })
  }
}
