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
package optimus.platform.relational.asm

import optimus.platform.relational.tree._

import scala.collection.mutable

object VariableBinder {
  def bind(e: RelationElement, dep: NodeDependency): AnalyzedTree = {
    val b = new VariableBinder
    b.visitElement(e)
    AnalyzedTree(dep.nodes, dep.dependencies, b.constants)
  }

  private def canGenByteCodeConstant(t: TypeInfo[_]): Boolean = {
    import TypeInfo._
    t match {
      case BOOLEAN | CHAR | BYTE | INT | SHORT | LONG | FLOAT | DOUBLE | STRING => true
      case _                                                                    => false
    }
  }
}

class VariableBinder extends QueryTreeVisitor {
  import VariableBinder._

  val constants = new BoundConstants

  protected override def handleConstValue(c: ConstValueElement): RelationElement = {
    if (c.value == null || canGenByteCodeConstant(c.rowTypeInfo)) c
    else if (c.value.isInstanceOf[Class[_]]) c
    else {
      constants.addReference(c.value, c.rowTypeInfo)
      c
    }
  }
}

final case class AnalyzedTree(
    nodes: Set[RelationElement],
    nodeDependencies: Map[RelationElement, List[RelationElement]],
    constants: BoundConstants)

class BoundConstants {
  val indexes = new mutable.HashMap[Any, Int]
  val values = new mutable.ListBuffer[Any]

  def addReference(value: Any, t: TypeInfo[_]): Unit = {
    if (!indexes.contains(value)) {
      indexes.put(value, values.size)
      values.append(value)
    }
  }
}
