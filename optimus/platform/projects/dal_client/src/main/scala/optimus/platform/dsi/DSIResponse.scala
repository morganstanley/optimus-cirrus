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
package optimus.platform.dsi

import optimus.dsi.session.EstablishSessionResult
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.VersioningResult
import optimus.platform.dsi.protobufutils.DalBrokerClient

sealed trait Response {
  def results: Seq[Result]
  def establishSessionResult: Option[EstablishSessionResult]
  // it is under discussion whether an individual result should ever result in requesting a failover,
  // if it shouldn't then we wouldn't need this client reference
  def client: Option[DalBrokerClient]
}

final case class DSIResponse(
    results: Seq[Result],
    establishSessionResult: Option[EstablishSessionResult],
    client: Option[DalBrokerClient] = None)
    extends Response

final case class VersioningResponse(result: VersioningResult) extends Response {
  override def results: Seq[Result] = Seq(result)
  override def establishSessionResult: Option[EstablishSessionResult] = None
  override def client: Option[DalBrokerClient] = None
}
