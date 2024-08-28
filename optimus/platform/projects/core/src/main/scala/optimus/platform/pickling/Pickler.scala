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

import optimus.exceptions.RTExceptionTrait

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Type ${T} does not seem to be supported as a stored property (can't find pickler).")
trait Pickler[T] {
  def pickle(t: T, visitor: PickledOutputStream): Unit
}

trait ExplicitOutputPickler[T, S] extends Pickler[T]

class PicklingException(val error: String, val cause: Option[Exception] = None)
    extends RuntimeException(error, cause.orNull)
    with RTExceptionTrait
