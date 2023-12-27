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
package optimus.platform.relational.dal.fullTextSearch

import java.util
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALQueryMethod
import optimus.platform.relational.dal.core.DALBinder
import optimus.platform.relational.dal.core.DALQueryBinder
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import optimus.platform.relational.data.mapping._

class FullTextSearchBinder(mapper: QueryMapper, root: RelationElement) extends DALBinder(mapper, root) {

  private val projected = new util.IdentityHashMap[ProjectionElement, RelationElement]

  protected override def handleQuerySrc(provider: ProviderRelation): RelationElement = {
    super.handleQuerySrc(provider) match {
      case p: ProjectionElement =>
        projected.put(p, provider)
        p
      case x => x
    }
  }

  override def bind(e: RelationElement): RelationElement = {
    val elem = visitElement(e)
    new TableScanCheck(projected).visitElement(elem)
  }

  override def handleMethod(method: MethodElement): RelationElement = {
    method.methodCode match {
      case DALQueryMethod.FullTextSearchOnly => bindFullTextSearchOnly(method)
      case _                                 => super.handleMethod(method)
    }
  }

  override def handleFuncCall(func: FuncElement): RelationElement = {
    val fn = super.handleFuncCall(func)
    fn match {
      case FuncElement(mc: MethodCallee, List(c: ColumnElement), v: ConstValueElement) =>
        mc.method.name match {
          case "isAfter"  => ElementFactory.lessThan(c, v)
          case "isBefore" => ElementFactory.greaterThan(c, v)
          case "isEqual"  => ElementFactory.equal(c, v)
          case _          => fn
        }
      case FuncElement(mc: MethodCallee, List(v: ConstValueElement), c: ColumnElement) =>
        mc.method.name match {
          case "isAfter"  => ElementFactory.lessThan(c, v)
          case "isBefore" => ElementFactory.greaterThan(c, v)
          case "isEqual"  => ElementFactory.equal(c, v)
          case _          => fn
        }
      case _ => fn
    }
  }

  protected def bindFullTextSearchOnly(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection ne null) {
      projected.put(projection, method)
      projection
    } else throw new RelationalUnsupportedException("Violate the 'fullTextSearch' execution constraint.")
  }

  protected override def bindPermitTableScan(method: MethodElement): RelationElement = {
    def checkProjection(method: MethodElement): RelationElement = {
      val (s :: others) = method.methodArgs
      val source = visitElement(s.param)
      val projection = convertToSequence(source)
      if (projection eq null) {
        if (source eq s.param) method else replaceArgs(method, others, source)
      } else {
        // we do not do this check for db yet, we by-pass it here
        projection
      }
    }
    checkProjection(method) match {
      case p: ProjectionElement =>
        projected.put(p, method)
        p
      case x => x
    }
  }

  private class TableScanCheck(map: util.IdentityHashMap[ProjectionElement, RelationElement])
      extends DbQueryTreeVisitor {
    protected override def handleProjection(proj: ProjectionElement): RelationElement = {
      if (proj.select.from.isInstanceOf[TableElement] && !hasFilterCondition(proj.select)) map.get(proj) else proj
    }
    protected def hasFilterCondition(s: SelectElement): Boolean = {
      if (s.where ne null) true
      else
        s.from match {
          case sel: SelectElement => hasFilterCondition(sel)
          case _                  => false
        }
    }
  }

}

object FullTextSearchBinder extends DALQueryBinder {
  def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
    new FullTextSearchBinder(mapper, e).bind(e)
  }
}
