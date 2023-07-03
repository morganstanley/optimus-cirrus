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
package optimus.platform.relational

import optimus.platform._
import optimus.platform._
import optimus.platform.relational.inmemory.ArrangedSource
import optimus.platform.relational.tree._

/**
 * arranged rule:
 *
 * 1 if the original element ProviderRelation'arrange is true it is arranged otherwise
 *
 * 2 if one of the elements in element chain is arranged or take it is arranged otherwise
 *
 * 3 if both branch in JoinRelationElement are arranged, it is arranged
 */
private class ArrangedInspector extends QueryTreeVisitor {
  // it is var because we need to assign it when traversing the whole tree.
  private[this] var isArranged = false

  def isArranged(e: RelationElement): Boolean = {
    isArranged = false
    visitElement(e)
    isArranged
  }

  override protected def handleMethod(m: MethodElement): RelationElement = {
    if (m.methodCode == QueryMethod.ARRANGE)
      isArranged = true
    else {
      isArranged = m.methodArgs.forall { arg =>
        arg.param match {
          case a: MultiRelationElement => isArranged(a)
          case _                       => true
        }
      }
    }
    m
  }

  override protected def handleQuerySrc(element: ProviderRelation): RelationElement = {
    element match {
      case a: ArrangedSource => isArranged = true
      case _                 =>
    }
    element
  }
}

object ArrangedInspector {
  def isArranged(e: RelationElement): Boolean = {
    val inspector = new ArrangedInspector
    inspector.isArranged(e)
  }

  def isKeyComparable(key: RelationKey[_]): Boolean = {
    if ((key eq null) || (key eq NoKey)) false
    else key.isKeyComparable
  }
}
