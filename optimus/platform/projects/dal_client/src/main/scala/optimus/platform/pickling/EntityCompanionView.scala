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
package optimus.platform.pickling

import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase
import optimus.graph.PropertyInfo0
import optimus.graph.ReallyNontweakablePropertyInfo
import optimus.graph.NodeKey
import optimus.platform._
import optimus.platform.EvaluationContext

class EntityCompanionView[T <: Entity](comp: EntityCompanionBase[T]) {
  def fromMap(m: Map[String, Any]): T = fromProperties(PickledProperties(m))
  def fromProperties(m: PickledProperties): T = {
    val p = new PickledMapWrapper(m)
    val entity = comp.info.createUnpickled(p, forceUnpickle = true).asInstanceOf[T]

    {
      // Ensure that all Entity Ref vals are hydrated
      // this is needed now because the new equality semantics
      // compare heap instances on their ctor args. For this
      // comparison to work, we need to ensure that all such vals
      // are hydrated by forcing their impl's to be called.
      // - Jan 15, 2013

      @entersGraph def hydrate(nodeKey: NodeKey[_]) = {
        // Cause hydration of an entity ref by causing the node to evaluate
        // via Node.get.
        val node = nodeKey.prepareForExecutionIn(EvaluationContext.scenarioStack)
        node.await // Not interested in the return value. Just doing this to cause
        // hydration of entity ref
      }

      for (p <- entity.$info.properties if p.isStored) {
        p match {
          // Could be @node val blah: SomeEntity
          case x: PropertyInfo0[_, _] =>
            hydrate(x.asInstanceOf[PropertyInfo0[T, _]].createNodeKey(entity))
          // Could be val blah: SomeEntity - Nice name for the property info btw(!)
          case x: ReallyNontweakablePropertyInfo[_, _] =>
            x.asInstanceOf[ReallyNontweakablePropertyInfo[T, _]].getValue(entity)
          case _ =>
        }
      }
    }

    entity
  }
}
