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
package optimus.platform.dsi.bitemporal.proto

import optimus.platform.priql.NamespaceWrapper
import optimus.platform.priql.RelationElementWrapper
import optimus.platform.relational.reactive.ReactiveQueryProcessorT
import optimus.platform.storable.Entity

// This trait breaks a dal_client dependency on platform.
trait TemporalSurfaceMatcherT {
  def classes: Set[Class[_ <: Entity]] = Set.empty
  def namespaceWrappers: Set[NamespaceWrapper] = Set.empty
  def reactiveQueryProcessors: Seq[ReactiveQueryProcessorT] = Seq.empty
  def relationElementWrappers: Seq[Seq[RelationElementWrapper]] = Seq.empty
}
