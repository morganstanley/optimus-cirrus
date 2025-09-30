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
package optimus.dist.breadcrumbs

import optimus.breadcrumbs.crumbs.KnownProperties
import optimus.scalacompat.collection._
import spray.json.DefaultJsonProtocol._

private[optimus] object DistProperties extends KnownProperties {
  val jobId = prop[String]("job_id")
  val taskId = prop[String]("task_id")
  val rootChainId = prop[String]("root_chain_id")
}
