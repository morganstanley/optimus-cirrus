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

import java.util.BitSet
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import scala.collection.mutable.HashMap

class RedundantColumnRemover private () extends DbQueryTreeVisitor {
  private val map = new HashMap[ColumnElement, ColumnElement]()

  override protected def handleColumn(column: ColumnElement): RelationElement = {
    map.getOrElse(column, column)
  }

  override protected def handleSelect(select: SelectElement): RelationElement = {
    val s = super.handleSelect(select).asInstanceOf[SelectElement]
    val cols = s.columns.sortBy(_.name)
    val removed = new BitSet(cols.size)

    def examineRedundant(cols: List[ColumnDeclaration], i: Int): Unit = {
      cols match {
        case Nil =>
        case cd :: tail =>
          if (!removed.get(i)) {
            val cde = cd.element
            lazy val cei = {
              val columnInfo = cde match {
                case c: ColumnElement => c.columnInfo
                case _                => ColumnInfo.Calculated
              }
              new ColumnElement(cde.rowTypeInfo, s.alias, cd.name, columnInfo)
            }
            for ((cdj, j) <- tail.zipWithIndex if !removed.get(j + i)) {
              if (sameElement(cde, cdj.element)) {
                // any reference to 'j' should now just be a reference to 'i'
                val cej = new ColumnElement(cdj.element.rowTypeInfo, s.alias, cdj.name, cei.columnInfo)
                map.put(cej, cei)
                removed.set(j + i + 1)
              }
            }
          }
          examineRedundant(tail, i + 1)
      }
    }

    examineRedundant(cols, 0)
    if (removed.nextSetBit(0) < 0) s
    else {
      val newDecls = cols.zipWithIndex.collect { case (cd, i) if !removed.get(i) => cd }
      new SelectElement(
        s.alias,
        newDecls,
        s.from,
        s.where,
        s.orderBy,
        s.groupBy,
        s.skip,
        s.take,
        s.isDistinct,
        s.reverse)
    }
  }

  private def sameElement(a: RelationElement, b: RelationElement): Boolean = (a, b) match {
    case (ca: ColumnElement, cb: ColumnElement) => ca.alias == cb.alias && ca.name == cb.name
    case _                                      => a eq b
  }
}

object RedundantColumnRemover {
  def remove(e: RelationElement): RelationElement = {
    new RedundantColumnRemover().visitElement(e)
  }
}
