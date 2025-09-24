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

/**
 * * Marker trait for a plugin tag of distribution param that will skip groupkey enrichment in batching decisions and batcher operation. These tags could
 *   allow better batch sizes.
 * 1. This pluginType must be used by the leaf node plugin, that means no further distribution/batching/throttling/database querying
 *    are spawn by such node. Otherwise, because other pluginTypes are ignored, no distribution/batching/throttling/database querying etc.
 *    will take effects on its descendants nodes.
 * 2. This pluginType must be used by the node plugin which doesn't recursively invoke itself, otherwise, a deadlock will happen.
 */
trait NonEnrichGroupKeyPluginTag
