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

import optimus.platform.storable.Entity

trait AuditVisitor {

  /**
   * Called on the completion of the given NodeTask
   * @param ntsk
   *   Just completed NodeTask
   * @param computedInfo
   *   All the collected information so far
   * @return
   *   Can be different then original
   */
  def visit(ntsk: NodeTask, computedInfo: NodeExtendedInfo): NodeExtendedInfo

  /**
   * Called on the Entity creation
   * @param e
   *   Entity being created
   * @param computedInfo
   *   All the collected information so far for the node currently executing
   * @return
   *   Can be different then original
   */
  def visit(e: Entity, computedInfo: NodeExtendedInfo): NodeExtendedInfo

  /**
   * Called before running a given NodeTask returned extendendInfo will be applied to the node before it is run
   *
   * @node
   *   is optional here. you can mark previsit @node to get async transformation, and safely ignore if you do not need
   *   it. DO NOT use @node unless you absolutely need it
   */
  protected def visitBeforeRun(v: NodeTask, info: NodeExtendedInfo): NodeExtendedInfo = ???
  protected def visitBeforeRun$newNode(v: NodeTask, info: NodeExtendedInfo): NodeKey[NodeExtendedInfo] = null
  protected def visitBeforeRun$queued(v: NodeTask, info: NodeExtendedInfo): NodeFuture[NodeExtendedInfo] = null

}
