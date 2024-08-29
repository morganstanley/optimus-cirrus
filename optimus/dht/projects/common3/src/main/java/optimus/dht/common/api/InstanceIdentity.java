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
package optimus.dht.common.api;

import javax.annotation.Nullable;

/** An interface used to provide an identity of a client or server. */
public interface InstanceIdentity {

  /**
   * A best effort unique identifier. It's very unlikely to be non-unique, but it's not guaranteed.
   */
  String uniqueId();

  /** Version of the framework code used by this instance. */
  String codeVersion();

  /** Fully qualified domain name of the host on which this instance is running. */
  String hostFQDN();

  /** PID of process in which this instance is running. */
  long pid();

  /**
   * Cloud name of the container on which this instance is running. <code>null</code> if not running
   * on cloud.
   */
  @Nullable
  String cloudName();
}
