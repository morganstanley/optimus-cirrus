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
package optimus.platform.relational.data.translation

import optimus.platform.relational.data.tree._
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.tree.RelationElement

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet
import java.util.HashMap

import optimus.platform.relational.tree.ElementType

/*
 * Result from calling ColumnProjector.projectColumns
 */
final case class ProjectedColumns(projector: RelationElement, columns: List[ColumnDeclaration])

/*
 * Splits an element into two parts
 * 1) a list of column declarations for sub-elements that must be evaluated on the server
 * 2) a element that describes how to combine/project the columns back together into the correct result
 */
class ColumnProjector protected (
    language: QueryLanguage,
    element: RelationElement,
    existingColumns: Seq[ColumnDeclaration],
    newAlias: TableAlias,
    existingAliases: Seq[TableAlias])
    extends DbQueryTreeVisitor {
  protected val columns = new ListBuffer[ColumnDeclaration]
  protected val columnNames = new HashSet[String]
  private[this] var iColumn = 0
  protected val map = new HashMap[ColumnElement, ColumnElement]
  protected val candidates: HashSet[RelationElement] = Nominator.nominate(language, element)

  if (existingColumns ne null) {
    columns ++= existingColumns
    columnNames ++= existingColumns.map(_.name)
  }

  override def visitElement(e: RelationElement): RelationElement = {
    if (e != null && (candidates.contains(e) || e.elementType == DbElementType.Column)) {
      e match {
        case column: ColumnElement =>
          var mapped = map.get(column)
          if (mapped ne null) mapped
          else {
            columns
              .collectFirst {
                case ColumnDeclaration(name, ce: ColumnElement) if ce.alias == column.alias && ce.name == column.name =>
                  new ColumnElement(column.projectedType(), newAlias, name, column.columnInfo)
              }
              .getOrElse {
                if (existingAliases.contains(column.alias)) {
                  val ordinal = columns.size
                  val columnName = getUniqueColumnName(column.name)
                  columns += ColumnDeclaration(columnName, column)
                  mapped = new ColumnElement(column.projectedType(), newAlias, columnName, column.columnInfo)
                  map.put(column, mapped)
                  columnNames += columnName
                  mapped
                } else column // must be referring to outer scope
              }
          }

        case _ =>
          val columnName = getNextColumnName()
          columns += ColumnDeclaration(columnName, e)
          columnNames += columnName
          new ColumnElement(e.projectedType(), newAlias, columnName, ColumnInfo.Calculated)
      }

    } else {
      super.visitElement(e)
    }
  }

  protected def getNextColumnName(): String = {
    iColumn += 1
    getUniqueColumnName(s"c$iColumn")
  }

  protected def getUniqueColumnName(name: String): String = {
    var newName = name
    val baseName = name
    var suffix = 1
    while (isColumnNameInUse(newName)) {
      newName = baseName + suffix
      suffix += 1
    }
    newName
  }

  protected def isColumnNameInUse(name: String): Boolean = {
    columnNames.contains(name)
  }
}

object ColumnProjector {
  def projectColumns(
      language: QueryLanguage,
      element: RelationElement,
      existingColumns: Seq[ColumnDeclaration],
      newAlias: TableAlias,
      existingAliases: Iterable[TableAlias]): ProjectedColumns = {
    val projector = new ColumnProjector(language, element, existingColumns, newAlias, existingAliases.toSeq)
    val elem = projector.visitElement(element)
    ProjectedColumns(elem, projector.columns.toList)
  }

  def projectColumns(
      language: QueryLanguage,
      element: RelationElement,
      existingColumns: Seq[ColumnDeclaration],
      newAlias: TableAlias,
      existingAliases: TableAlias*): ProjectedColumns = {
    projectColumns(language, element, existingColumns, newAlias, existingAliases)
  }
}

/*
 * Nominator is a class that walks an relation element tree bottom up, determining the set of
 * candidate elements that are possible columns of a select element
 */
private class Nominator(language: QueryLanguage) extends DbQueryTreeVisitor {
  private val candidates = new HashSet[RelationElement]
  private[this] var isBlocked = false

  override def visitElement(e: RelationElement): RelationElement = {
    if (e ne null) {
      val saveIsBlocked = isBlocked
      isBlocked = false
      if ((language.mustBeColumn(e))) {
        candidates += e
      } else {
        super.visitElement(e)
        if (!isBlocked) {
          if (language.canBeColumn(e))
            candidates += e
          else
            isBlocked = true
        }
        isBlocked |= saveIsBlocked
      }
    }
    e
  }

  protected override def handleProjection(proj: ProjectionElement): RelationElement = {
    visitElement(proj.projector)
    proj
  }
}

private object Nominator {
  def nominate(language: QueryLanguage, e: RelationElement): HashSet[RelationElement] = {
    val nominator = new Nominator(language)
    nominator.visitElement(e)
    nominator.candidates
  }
}
