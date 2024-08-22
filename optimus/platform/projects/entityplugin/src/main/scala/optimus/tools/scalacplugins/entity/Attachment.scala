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
package optimus.tools.scalacplugins.entity

import scala.reflect.internal.util.TriState
import scala.tools.nsc.Global

object Attachment {
  sealed trait NodeType
  object NodeType {
    // Explicitly non-RT - should not be reordered in async blocks
    final case class NonRTNode(scenarioIndependent: Boolean, createNodeTrait: Boolean) extends NodeType
    // May be RT, but not a full property node (no PropertyInfo generated, and not cached). Note
    // that raw nodes may still be considered SI - this attribute won't be used for caching purposes
    // (since raw nodes aren't cached), but will still be used as part of SI -> non-SI checks in
    // optimus_refchecks.
    // note that raw nodes never can be tweakable, but they can be non-tweakable (such as on a trait which is then
    // extended by an entity)
    final case class RawNode(scenarioIndependent: Boolean, createNodeTrait: Boolean, notTweakable: Boolean)
        extends NodeType
    // RT, PropertyInfo generated
    final case class PropertyNode(stability: PropertyStability) extends NodeType
    object PropertyNode {
      def apply(si: Boolean, tweakable: TriState): PropertyNode =
        PropertyNode(PropertyStability(si, tweakable))
    }
    // RT, PropertyInfo generated
    final case class ValNode(tweakable: TriState) extends NodeType
  }

  sealed trait PropertyStability
  object PropertyStability {
    def apply(si: Boolean, tweakable: TriState): PropertyStability = ((si, tweakable): @unchecked) match {
      case (true, _)                 => ScenarioIndependent
      case (false, TriState.False)   => NonTweakable
      case (false, TriState.Unknown) => MaybeTweakable
      case (false, TriState.True)    => Tweakable
    }

    case object ScenarioIndependent extends PropertyStability
    case object NonTweakable extends PropertyStability
    case object MaybeTweakable extends PropertyStability
    case object Tweakable extends PropertyStability
  }

  // Note: This attachment is only added to nodes and stored vals
  sealed trait NodeFieldType
  object NodeFieldType {
    final case class BareVal() extends NodeFieldType
    final case class BackingVal(getterSym: Global#Symbol) extends NodeFieldType
    final case class GetterDef(backingValSym: Global#Symbol) extends NodeFieldType
    final case class PlainDef() extends NodeFieldType
  }

  final case class Node(nodeType: NodeType, async: Boolean) {
    def isVal: Boolean = nodeType.isInstanceOf[NodeType.ValNode]
    def isDef: Boolean = !isVal

    def rt: TriState = nodeType match {
      case _: NodeType.NonRTNode    => TriState.False
      case _: NodeType.RawNode      => TriState.Unknown
      case _: NodeType.PropertyNode => TriState.True
      case _: NodeType.ValNode      => TriState.True
    }

    def raw: Boolean = nodeType.isInstanceOf[NodeType.NonRTNode] || nodeType.isInstanceOf[NodeType.RawNode]

    def scenarioIndependent: Boolean = nodeType match {
      case NodeType.ValNode(TriState.False)                             => true
      case NodeType.PropertyNode(PropertyStability.ScenarioIndependent) => true
      case NodeType.RawNode(si, _, _)                                   => si
      case NodeType.NonRTNode(si, _)                                    => si
      case _                                                            => false
    }

    def nonRTWithCreateNodeTrait: Boolean = nodeType match {
      case NodeType.NonRTNode(_, createNodeTrait) => createNodeTrait
      case _                                      => false
    }

    def tweakable: TriState = nodeType match {
      case NodeType.PropertyNode(PropertyStability.Tweakable) => TriState.True
      case NodeType.PropertyNode(PropertyStability.NonTweakable | PropertyStability.ScenarioIndependent) =>
        TriState.False
      case NodeType.ValNode(tweakable)               => tweakable
      case raw: NodeType.RawNode if raw.notTweakable => TriState.False
      case _                                         => TriState.Unknown
    }
  }

  sealed trait Stored {
    def lazyLoaded: Boolean
  }
  // StoredGetter is either a bare ValDef or a DefDef accessor
  final case class StoredGetter(lazyLoaded: Boolean) extends Stored
  final case class StoredBackingVal(getterSym: Global#Symbol, lazyLoaded: Boolean) extends Stored

  case object ForceNodeClass {
    private val tag = reflect.classTag[this.type]
    def put(sym: Global#Symbol): Unit = { sym.updateAttachment(this)(tag) }
    def on(sym: Global#Symbol): Boolean = sym.hasAttachment(tag)
  }
}
