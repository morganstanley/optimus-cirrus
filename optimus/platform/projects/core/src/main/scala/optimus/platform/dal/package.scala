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
package optimus.platform

import java.io.Serializable

import optimus.graph.AlreadyCompletedPropertyNode
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTaskInfo._
import optimus.graph.PropertyNode
import optimus.platform.internal.MarkerResolver
import optimus.platform.storable.Entity

package object dal {
  val OPTIMUS = "OPTIMUS"

  final val MarkerTaskInfo = new NodeTaskInfo("[tweak_marker]", TWEAKABLE | PROFILER_INTERNAL)
  MarkerTaskInfo.internalSetTweakMaskForDAL()

  private[optimus] object Marker {
    private val lookup = scala.collection.mutable.Map.empty[MarkerResolver[_], Marker[_]]
    private[optimus] def apply[T <: AnyRef](entity: Entity, resolver: MarkerResolver[_]): PropertyNode[T] =
      lookup.synchronized {
        require(!lookup.contains(resolver))
        val ret = new Marker[T](entity, resolver)
        lookup.put(resolver, ret)
        ret
      }
    // we send the information as this class, because there is nothing to stop the enum actually being transmitted
    // so this class means that the purpose was to be resolved
    private final class MarkerSerialisedForm(resolver: MarkerResolver[_]) extends Serializable {
      // noinspection ScalaUnusedSymbol
      private def readResolve: Any =
        lookup.getOrElse(resolver, throw new IllegalStateException("no registration for " + resolver))
    }
  }
  final class Marker[T <: AnyRef] private (entity: Entity, resolver: MarkerResolver[_])
      extends AlreadyCompletedPropertyNode[T](null.asInstanceOf[T], entity, MarkerTaskInfo) {
    override def hashCode: Int = System.identityHashCode(this)
    override def equals(o: Any): Boolean = o.isInstanceOf[AnyRef] && (o.asInstanceOf[AnyRef] eq this)
    // noinspection ScalaUnusedSymbol
    private def writeReplace: Any = new Marker.MarkerSerialisedForm(resolver)
  }

  private[optimus] val DalChronon = 1000L
}
