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
package optimus.platform.storable

import scala.util.DynamicVariable
import scala.collection.mutable

object SerializationReferenceCollector extends DynamicVariable[mutable.HashSet[TemporalEntityReference]](null) {
  def add(e: Entity): Unit = {
    val buf = this.value
    if (buf == null) {
      e.log
        .debug("Serializing entity without SerializationReferenceCollector: Could result in non-batched DAL access")
      // throw new NullPointerException("Cannot serialize Entity without using
      // SerializationReferenceCollector")
    } else buf += TemporalEntityReference(e.dal$entityRef, e.dal$temporalContext, e.getClass)
  }
}
