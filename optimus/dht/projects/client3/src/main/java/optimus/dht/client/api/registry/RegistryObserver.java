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
package optimus.dht.client.api.registry;

import java.time.Duration;

/**
 * Instances of this interface are used to register listeners, which are informed about about any
 * changes in the dht servers list.
 */
public interface RegistryObserver {

  /**
   * Registers new listener for this observer.
   *
   * @param listener
   */
  void registerListener(RegistryNodeChangesListener listener);

  /**
   * Blocks until observer becomes consistent for the first time. Does nothing if observer is
   * already consistent or was consistent before.
   *
   * @param timeout how long to wait
   * @throws InterruptedException
   */
  void waitUntilConsistentForFirstTime(Duration timeout) throws InterruptedException;

  /**
   * Returns if this observer is currently consistent - that is, if it's up to date with the
   * underlying registry.
   *
   * @return if this observer is currently consistent
   */
  boolean isConsistent();
}
