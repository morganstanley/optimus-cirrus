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
// docs-snippet:PIIElementUnpickler
package optimus.platform.pickling

import optimus.datatype.Classification.DataSubjectCategory
import optimus.datatype.FullName
import optimus.datatype.PIIElement
import optimus.platform.annotations.nodeSync

class PIIElementUnpickler[T <: DataSubjectCategory, E <: PIIElement[T]](createInstance: String => E)
    extends Unpickler[E] {
  @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream): E = {
    pickled match {
      case value: String => createInstance(value)
    }
  }
}
// docs-snippet:PIIElementUnpickler
