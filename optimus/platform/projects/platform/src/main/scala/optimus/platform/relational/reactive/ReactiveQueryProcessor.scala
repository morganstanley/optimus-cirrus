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
package optimus.platform.relational.reactive

import optimus.entity.ClassEntityInfo
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.{
  BinaryExpressionElement,
  BinaryExpressionType,
  MemberElement,
  MultiRelationElement,
  RelationElement
}

/**
 * for allFilters in a Priql query, we filter out two *mutex* set of filters:
 *   1. nonUniqueIndex, e.g. filter on @indexed field, send this query to server side with historical API. then we
 *      filter result on client side against the rest filters 2. uniqueIndex, e.g. filter on @key field, send this query
 *      to server side with old range query implementation, because DAL historical API getEntityVersionsInRange behave
 *      different with old range- query (cannot return rect of last transaction). then we filter result on client side
 *      against the rest filters
 *
 * @param entityInfo
 *   entity info to figure out indexes on an entity
 * @param allFilters
 *   allFilters parsed from priql range query
 */
class ReactiveQueryProcessor(val entityInfo: ClassEntityInfo, val allFilters: List[RelationElement])
    extends ReactiveQueryProcessorT {

  override def toString: String = s"ReactiveQueryProcessor($entityInfo, $allFilters)"

  lazy val (nonUniqueindexedFilter, restFiltersForNonUniqueIndex) = {
    // we will send non-unique key query through findIndexByRange which only support non-unique key
    def isNonUniqueKey(elem: RelationElement): Boolean = {
      elem match {
        case MemberElement(_, name) => entityInfo.indexes.exists(index => index.name == name && !index.unique)
        case _                      => false
      }
    }
    val (indexed, complex) = allFilters.partition {
      case b: BinaryExpressionElement if b.op == BinaryExpressionType.EQ || b.op == BinaryExpressionType.ITEM_IS_IN =>
        val left = isNonUniqueKey(b.left)
        val right = isNonUniqueKey(b.right)
        left ^ right
      case f: FuncElement if f.callee.name == "contains" =>
        isNonUniqueKey(f.arguments.head)
      case _ => false
    }
    (indexed.headOption, if (indexed.size <= 1) complex else indexed.tail ++ complex)
  }

  lazy val (allIndexAndKeyFilter, restFiltersForIndex) = {
    def isIndex(elem: RelationElement): Boolean = {
      elem match {
        case MemberElement(_, name) => entityInfo.indexes.exists(index => index.name == name)
        case _                      => false
      }
    }
    val (indexed, complex) = allFilters.partition {
      case b: BinaryExpressionElement if b.op == BinaryExpressionType.EQ || b.op == BinaryExpressionType.ITEM_IS_IN =>
        val left = isIndex(b.left)
        val right = isIndex(b.right)
        left ^ right
      case f: FuncElement if f.callee.name == "contains" =>
        isIndex(f.arguments.head)
      case _ => false
    }
    (indexed.headOption, if (indexed.size <= 1) complex else indexed.tail ++ complex)
  }
}

object ReactiveQueryProcessor {
  def buildFromRelation(relation: MultiRelationElement): ReactiveQueryProcessor = {
    val qps: QueryParseResult = NotificationQueryParser.parse(relation)
    new ReactiveQueryProcessor(qps.entityInfo, qps.allFilters)
  }
}
