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
package optimus.platform.relational.dal.core

import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.translation.SourceColumnFinder
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.DbEntityElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.EmbeddableCaseClassElement
import optimus.platform.relational.data.tree.EmbeddableCollectionElement
import optimus.platform.relational.data.tree.KnowableValueElement
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType
import optimus.platform.relational.tree.ConditionalElement
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.RelationElement

class SpecialElementRewriter private (private val searchScope: RelationElement) extends DbQueryTreeVisitor {
  private[this] var currentFrom: RelationElement = _
  private[this] var inSelect = false

  protected override def handleSelect(select: SelectElement): RelationElement = {
    val saveInSelect = inSelect
    val saveFrom = currentFrom
    inSelect = true
    currentFrom = visitSource(select.from)

    val where = visitElement(select.where)
    val orderBy = visitOrderBy(select.orderBy)
    val groupBy = visitElementList(select.groupBy)
    val skip = visitElement(select.skip)
    val take = visitElement(select.take)
    val columns = visitColumnDeclarations(select.columns)

    val s = updateSelect(select, currentFrom, where, orderBy, groupBy, skip, take, columns)
    currentFrom = saveFrom
    inSelect = saveInSelect
    s
  }

  protected override def handleDbEntity(entity: DbEntityElement): RelationElement = {
    visitElement(entity.element)
  }

  protected override def handleEmbeddableCollection(collection: EmbeddableCollectionElement): RelationElement = {
    // if it was not expanded (relationship) yet, we no longer need to do it.
    visitElement(collection.element)
  }

  protected override def handleEmbeddableCaseClass(ec: EmbeddableCaseClassElement): RelationElement = {
    if (inSelect) visitElement(ec.members.head)
    else
      new EmbeddableCaseClassElement(
        ec.owner,
        ec.ownerProperty,
        ec.projectedType(),
        ec.members.take(1),
        ec.memberNames.take(1))
  }

  protected override def handleDALHeapEntity(entity: DALHeapEntityElement): RelationElement = {
    if (inSelect) visitElement(entity.members.head)
    else
      new DALHeapEntityElement(
        entity.companion,
        entity.projectedType(),
        entity.members.take(1),
        entity.memberNames.take(1))
  }

  protected override def handleConditional(c: ConditionalElement): RelationElement = {
    import BinaryExpressionType._

    def asColumnOpt(e: RelationElement): Option[ColumnElement] = {
      e match {
        case c: ColumnElement                       => Some(c)
        case KnowableValueElement(c: ColumnElement) => Some(c)
        case _                                      => None
      }
    }

    super.handleConditional(c) match {
      case cond @ ConditionalElement(
            test,
            ConstValueElement(false, _),
            ifFalse @ BinaryExpressionElement(EQ, c1, c2, _),
            _) =>
        val c0Opt = test match {
          case BinaryExpressionElement(EQ, c0: ColumnElement, ConstValueElement(null, _), _) =>
            if (c0.name.startsWith(DALProvider.EntityRef)) Some(c0)
            else
              (c1, c2) match {
                case (`c0`, c: ConstValueElement) if c.value != null => Some(c0)
                case (c: ConstValueElement, `c0`) if c.value != null => Some(c0)
                case _                                               => None
              }
          case BinaryExpressionElement(EQ, he: DALHeapEntityElement, ConstValueElement(null, _), _) =>
            Some(he.members.head.asInstanceOf[ColumnElement])
          case _ => None
        }
        c0Opt map { c0 =>
          val scOpt0 = SourceColumnFinder.findSourceColumn(searchScope, c0)
          val scOpt1 = asColumnOpt(c1).flatMap(c => SourceColumnFinder.findSourceColumn(searchScope, c))
          val scOpt2 = asColumnOpt(c2).flatMap(c => SourceColumnFinder.findSourceColumn(searchScope, c))
          (scOpt0, scOpt1, scOpt2) match {
            case (Some(sc0), Some(sc1), _) if (sc0.alias eq sc1.alias) =>
              c2 match {
                case ConstValueElement(null | None, _) => cond
                case _                                 => ifFalse
              }
            case (Some(sc0), _, Some(sc2)) if (sc0.alias eq sc2.alias) =>
              c1 match {
                case ConstValueElement(null | None, _) => cond
                case _                                 => ifFalse
              }
            case _ => cond
          }
        } getOrElse (cond)

      case cond @ ConditionalElement(test, ConstValueElement(null, _), c1: ColumnElement, _) =>
        test match {
          case BinaryExpressionElement(EQ, c2: ColumnElement, ConstValueElement(null, _), _)
              if c1.alias == c2.alias && c2.name.startsWith(DALProvider.EntityRef) =>
            val scOpt1 = SourceColumnFinder.findSourceColumn(searchScope, c1)
            val scOpt2 = SourceColumnFinder.findSourceColumn(searchScope, c2)
            (scOpt1, scOpt2) match {
              case (Some(sc1), Some(sc2)) if sc1.alias eq sc2.alias => c1
              case _                                                => cond
            }
          case BinaryExpressionElement(EQ, he: DALHeapEntityElement, ConstValueElement(null, _), _)
              if he.members.head.asInstanceOf[ColumnElement].alias == c1.alias =>
            val c2 = he.members.head.asInstanceOf[ColumnElement]
            val scOpt1 = SourceColumnFinder.findSourceColumn(searchScope, c1)
            val scOpt2 = SourceColumnFinder.findSourceColumn(searchScope, c2)
            (scOpt1, scOpt2) match {
              case (Some(sc1), Some(sc2)) if sc1.alias eq sc2.alias => c1
              case _                                                => cond
            }
          case _ => cond
        }
      case cond => cond
    }
  }
}

object SpecialElementRewriter {
  def rewrite(e: RelationElement, searchScope: Option[RelationElement] = None): RelationElement = {
    new SpecialElementRewriter(searchScope.getOrElse(e)).visitElement(e)
  }
}
