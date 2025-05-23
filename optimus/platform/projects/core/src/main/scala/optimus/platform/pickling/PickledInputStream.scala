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

/**
 * This interface has 'seek' methods instead of 'read' methods. Eventually 'seek' returns boolean so to enable detection
 * of nonexistent primitive fields:
 *
 * if(in.seekInt("property") { foo = in.intValue } else { foo = 5 // default value }
 */
trait PickledInputStream {
  def temporalContext: TemporalContext

  def seek[T](k: String, unpickler: Unpickler[T]): Boolean
  def seekChar(k: String): Boolean
  def seekDouble(k: String): Boolean
  def seekFloat(k: String): Boolean
  def seekInt(k: String): Boolean
  def seekLong(k: String): Boolean

  def seekRaw(k: String): Boolean

  def inlineEntitiesByRef: InlineEntityHolder = InlineEntityHolder.empty
  def reference: StorableReference

  def charValue: Char
  def doubleValue: Double
  def floatValue: Float
  def intValue: Int
  def longValue: Long
  def value: Any
}
