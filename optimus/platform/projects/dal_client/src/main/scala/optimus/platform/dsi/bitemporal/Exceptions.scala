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
package optimus.platform.dsi.bitemporal

import optimus.platform.dsi.protobufutils.DalBrokerClient

/**
 * Exception thrown when DAL client fails to get response for request and under certain cases the request is retry-able
 * The client field can be used to shutdown the DAL client on which this exception occurred.
 */
class DALRetryableActionException(message: String, cause: Throwable, val client: Option[DalBrokerClient])
    extends DSIException(message, cause) {
  def this(msg: String, client: Option[DalBrokerClient]) = this(msg, null, client)
}

/**
 * Exception thrown when DAL client fails to send request or get response for request where the request is not
 * retry-able
 */
class DALNonRetryableActionException(message: String, cause: Throwable, val client: Option[DalBrokerClient])
    extends DSIException(message, cause) {
  def this(msg: String, client: Option[DalBrokerClient]) = this(msg, null, client)
}
