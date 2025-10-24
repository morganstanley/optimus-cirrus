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

import optimus.platform.TemporalContext
import optimus.platform.storable.InlineEntityHolder
import optimus.platform.storable.StorableReference

// TODO (OPTIMUS-78768): This is really a context for the unpickling, and not a stream. It should be renamed.
trait PickledInputStream {
  def temporalContext: TemporalContext
  def inlineEntitiesByRef: InlineEntityHolder = InlineEntityHolder.empty
  def reference: StorableReference
  def newMutStream: PickledInputStreamMut
}

object PickledInputStream {
  val empty: PickledInputStream = new PickledMapWrapper(PickledProperties.empty)
}
