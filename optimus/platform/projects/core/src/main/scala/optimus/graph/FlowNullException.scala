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
package optimus.graph

import optimus.exceptions.RTExceptionTrait

/**
 * FlowNullException is a "fake" exception used by UI apps. It can be thrown by nodes in order to display a blank value
 * in a widget that's bound to a type which doesn't have a value that can represent 'empty' (eg. Int).
 */
class FlowNullException(private val info: NodeTaskInfo)
    extends RuntimeException()
    with RTExceptionTrait
    with FlowControlException {
  def this() = this(null)
  def getSourceNode: String = if (info eq null) "node result" else s"${info.entityInfo}.${info.name}"

  override def getMessage: String = f"FlowNullException: ${getSourceNode} is None"
}

object FlowNullException {

  /** just throws a new FlowNullException but pretends the return type is T (to avoid awkward type ascription elsewhere) */
  def throwNew[T](): T = throw new FlowNullException()
}
