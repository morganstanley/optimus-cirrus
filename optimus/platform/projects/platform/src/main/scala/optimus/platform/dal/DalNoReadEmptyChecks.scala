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
package optimus.platform.dal

import optimus.core.needsPlugin
import optimus.graph.NodeKey
import optimus.graph.PluginSupport
import optimus.graph.PropertyNode
import optimus.platform.annotations.nodeLift
import optimus.platform.pickling.MaybePickledReference
import optimus.platform.storable.Entity
import optimus.platform.storable.StorableReference

import scala.collection.GenTraversable

/**
 * This interface contains wrappers around plugin internals that allow one to evaluate whether certain collection types,
 * containing entities, are empty without hydrating them. The underlying internals are currently a little unsafe and
 * were not originally intended to be used in this way. For this reason the interface presented is quite restricted. The
 * purpose of introducing the api is to support the train metrics job
 */
object NoReadEmptyChecks {

  /**
   * Checks to see if the option is empty without retrieving the underlying entities
   */
  @nodeLift
  def isOptionEmpty[T <: Entity](node: Option[T]): Boolean = needsPlugin
  def isOptionEmpty$node[T <: Entity](node: NodeKey[Option[T]]): Boolean = checkIsEmpty(node)

  /**
   * Checks to see if the option is empty without retrieving the underlying entities
   */
  @nodeLift
  def isOptionIterEmpty[T <: Entity](node: Option[Iterable[T]]): Boolean = needsPlugin
  def isOptionIterEmpty$node[T <: Entity](node: NodeKey[Option[Iterable[T]]]): Boolean = checkIsEmpty(node)

  /**
   * Checks to see if the iterable is empty without retrieving the underlying entities
   */
  @nodeLift
  def isIterEmpty[T <: Entity](node: Iterable[T]): Boolean = needsPlugin
  def isIterEmpty$node[T <: Entity](node: NodeKey[Iterable[T]]): Boolean = checkIsEmpty(node)

  /**
   * Checks to see if the outer iterable is empty without retrieving the underlying entities
   */
  @nodeLift
  def isIterIterEmpty[T <: Entity](node: Iterable[Iterable[T]]): Boolean = needsPlugin
  def isIterIterEmpty$node[T <: Entity](node: NodeKey[Iterable[Iterable[T]]]): Boolean = checkIsEmpty(node)

  // Unsafe type of NodeKey is not exposed to public API, its limited in the above
  // TODO (OPTIMUS-17589): Change this when getEntityReference returns the correct type
  private def checkIsEmpty(node: NodeKey[_]): Boolean = {
    node match {
      case l: MaybePickledReference[_] if !l.propertyInfo.isDirectlyTweakable =>
        PluginSupport.getEntityReference(node).isEmpty
      case _ =>
        throw new IllegalArgumentException(
          s"Can only use checkIsEmpty method on non tweakable lazy pickled references ${node.getClass}")
    }
  }

  /**
   * Checks to see if there are no entities in the compound collection at all without retrieving the underlying entities
   */
  @nodeLift
  def isIterIterEmptyFlattened[T <: Entity](node: Iterable[Iterable[T]]): Boolean = needsPlugin
  def isIterIterEmptyFlattened$node[T <: Entity](node: NodeKey[Iterable[Iterable[T]]]): Boolean =
    checkIsEmptyFlattened(node)

  /**
   * Checks to see if there are no entities inside the compound collection without retrieving the underlying entities
   */
  @nodeLift
  def isOptionIterEmptyFlattened[T <: Entity](node: Option[Iterable[T]]): Boolean = needsPlugin
  def isOptionIterEmptyFlattened$node[T <: Entity](node: NodeKey[Option[Iterable[T]]]): Boolean =
    checkIsEmptyFlattened(node)

  // Unsafe type of NodeKey is not exposed to public API, its limited in the above
  // TODO (OPTIMUS-17589): Change this when getEntityReference returns the correct type
  private def checkIsEmptyFlattened(node: NodeKey[_]): Boolean = {
    node match {
      case l: MaybePickledReference[_] if !l.propertyInfo.isDirectlyTweakable =>
        val a = PluginSupport.getEntityReference(node)
        // despite getEntityReference being declared to return Seq[EntityReference] it actually contains more structure than
        // that. This is all a little unsafe :0( I'm casting to the most general type required to perform the flatten operation
        // but in future this may need to change. Test coverage for all the current uses will fail if this cast becomes wrong
        // TODO (OPTIMUS-17589): Change this when getEntityReference returns the correct type
        a.asInstanceOf[GenTraversable[GenTraversable[StorableReference]]].flatten.isEmpty
      case _ =>
        throw new IllegalArgumentException(
          s"Can only use checkIsEmptyFlattened method on non tweakable lazy pickled references ${node.getClass}")
    }
  }
}
