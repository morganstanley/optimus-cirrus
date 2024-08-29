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
package optimus.platform.dal.prc

/**
 * An enumeration of reasons that we might have been asked to redirect a query originally sent to PRC to an alternative
 * service instead. The reason can be used to decide what redirection to perform.
 */
sealed trait RedirectionReason {
  final def name: String = toString
}

object RedirectionReason {

  /**
   * Indicates that a reason was received which we could not deserialize from proto -- most likely, the addition of a
   * new reason code which isn't yet supported on the client.
   */
  case object Unknown extends RedirectionReason

  /**
   * Indicates that a PRC server is unable to serve queries of the type that were supplied. This is data-independent;
   * the same reason will be returned for all future queries of this type regardless of whether e.g. temporality is
   * changed.
   */
  case object QueryTypeNotSupported extends RedirectionReason

  /**
   * Indicates that an unsupported temporality type was sent from the client. This is data-independent; the same reason
   * will be returned for all future queries which supply this same temporality type, regardless of the query type.
   */
  case object TemporalityTypeNotSupported extends RedirectionReason

  /**
   * Indicates that an unsupported command type was sent from the client. This is data-independent; the same reason will
   * be returned for all future queries which supply this same command type.
   */
  case object CommandTypeNotSupported extends RedirectionReason

  /**
   * Indicates that the PRC server could not (de-)serialize a PrcKey. Most likely this suggests a compatibility problem
   * between the client and the server. This may be data-dependent and so is not safe to cache.
   */
  case object PrcKeySerdeFailure extends RedirectionReason

  /**
   * Indicates that a PRC server failed to retrieve entities locally within a supported temporality and query type. Some
   * possible reasons:
   *   - the key is present from replication but we are querying at a temporality before we have data
   *   - the key is not present from replication In cases like these, PRC read-through might have served the query had
   *     it been enabled, but it was not. When a client receives a redirection such as this, it is not safe for it to
   *     cache it as it is dependent on the state of the PRC cluster rather than inherent to the nature of the query
   *     that was issued.
   */
  case object PrcReadThroughNotEnabled extends RedirectionReason

  /**
   * Indicates that the PRC server got some bitemporal query results but was then unable to fetch payloads for those
   * query results from the payload store, and so could not return a result to the client. This may be data-dependent
   * and is not safe to cache.
   */
  case object PayloadRetrievalFailure extends RedirectionReason

  /**
   * Indicates that the PRC server was not able to verify that the entitlements available satisfy the set required by
   * the query. For example, a linkage might grant the required entitlement, but PRC can't validate these yet, so it is
   * not safe to return a value to the user. A read broker may satisfy the query, however.
   */
  case object EntitlementsRequireFurtherChecks extends RedirectionReason

  /**
   * Indicates that the PRC server could not load the session from dht for the key hash it receives from the client
   */
  case object DhtSessionRetrievalFailure extends RedirectionReason

  /**
   * Indicates that PRC has performed a read-through but got some unexpected failure. Note that an ErrorResult from the
   * broker won't fall into this category; it is sent back to the DAL client as-is.
   */
  case object PrcReadThroughFailed extends RedirectionReason

  /**
   * Indicates that the PRC server needed a read-through to execute the request, but that no brokers were available to
   * serve that read-through. In this situation, the client is redirected to a broker to perform the request on the
   * grounds that the brokers which serve client requests should be available. Since the unavailability of brokers is
   * not request-dependent, this redirection could be transient and should not be cached.
   */
  case object PrcNoBrokersFailure extends RedirectionReason

  /**
   * Indicates that the PRC server needed a read-through to execute the request, but that an internal timeout was
   * reached while waiting for the read-through to be executed. In this situation, the client is directed to retry the
   * request against a read broker directly. This redirection should not be cached as the timeout may be unrelated to
   * the request being executed.
   */
  case object PrcReadThroughTimeoutFailure extends RedirectionReason
}
