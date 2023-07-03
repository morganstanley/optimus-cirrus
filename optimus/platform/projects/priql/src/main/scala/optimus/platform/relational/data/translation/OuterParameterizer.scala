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

import java.util

import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree.ParameterElement
import optimus.platform.relational.tree.RelationElement

import scala.collection.mutable

private class OuterParameterizer(outerAliases: mutable.HashSet[TableAlias]) extends DbQueryTreeVisitor {
  private[this] var iParam = 0
  private val map = new util.HashMap[RelationElement, RelationElement] // NamedValueElement

  protected override def handleProjection(proj: ProjectionElement): RelationElement = {
    val sel = visitElement(proj.select).asInstanceOf[SelectElement]
    updateProjection(proj, sel, proj.projector, proj.aggregator)
  }

  protected override def handleOptionElement(option: OptionElement): RelationElement = {
    option.element match {
      case c: ColumnElement => getNamedValueElement(c.alias, option).getOrElse(super.handleOptionElement(option))
      case _                => super.handleOptionElement(option)
    }
  }

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    getNamedValueElement(column.alias, column).getOrElse(column)
  }

  private def getNamedValueElement(alias: TableAlias, e: RelationElement): Option[RelationElement] = {
    if (outerAliases.contains(alias)) {
      var nv = map.get(e)
      if (nv eq null) {
        nv = NamedValueElement("n" + iParam, e)
        map.put(e, nv)
        iParam += 1
      }
      Some(nv)
    } else None
  }
}

// Convert references to outer alias to NamedValueElement
object OuterParameterizer {
  def parameterize(scope: Scope, e: RelationElement): RelationElement = {
    val outerAliases = new mutable.HashSet[TableAlias]
    var s = scope
    while (s ne null) {
      outerAliases.add(s.alias)
      s = s.outer
    }
    parameterize(outerAliases, e)
  }

  def parameterize(outerAliases: mutable.HashSet[TableAlias], e: RelationElement): RelationElement = {
    val op = new OuterParameterizer(outerAliases)
    op.visitElement(e)
  }
}

class Scope(
    val outer: Scope,
    val fieldReader: ParameterElement,
    val alias: TableAlias,
    val columns: List[ColumnDeclaration]) {
  val nameMap = columns.map(_.name).zipWithIndex.toMap

  def getValue(column: ColumnElement): Option[(ParameterElement, Int)] = {
    var s = this
    var res: Option[(ParameterElement, Int)] = None
    while ((s ne null) && !res.isDefined) {
      if ((column.alias eq s.alias) && s.nameMap.contains(column.name)) {
        val ordinal = s.nameMap(column.name)
        res = Some(s.fieldReader, ordinal)
      }
      s = s.outer
    }
    res
  }
}
