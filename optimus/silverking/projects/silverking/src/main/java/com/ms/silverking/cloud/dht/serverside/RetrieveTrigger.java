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
package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.ms.silverking.cloud.dht.common.DHTKey;

public interface RetrieveTrigger extends Trigger {
  /**
   * A {@link Callback} trigger owns responsibility for invoking a {@link RetrieveCallback} at a time of its choosing.
   * The {@code callback} passed to the trigger will wrap SilverKing server behaviour such as response handling and as
   * such this gives the ability for a {@link Callback} trigger to hand off responsibility for parts of execution to
   * other threads.
   * <p>
   * Note that these triggers are invoked without the {@link com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore}
   * read lock first being acquired; where {@code Callback} triggers invoke operations against the namespace it is their
   * responsibility to handle locking these accordingly.
   * <p>
   * For quick-running or simple triggers, you may wish to consider a {@link} Direct trigger instead.
   */
  interface Callback extends RetrieveTrigger {
    <T> T retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options,
        RetrieveCallback<IntermediateResult, T> callback);

    <T> T retrieve(SSNamespaceStore nsStore, DHTKey[] keys, SSRetrievalOptions options,
        RetrieveCallback<IntermediateResult[], T> callback);
  }

  /**
   * A {@link Direct} trigger is invoked by the {@link com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore} once
   * it has acquired the namespace read lock, and must return the result of the retrieval from its {@code retrieve}
   * method. Trigger authors who  wish to do longer-running processing, including handing off request handling to other
   * threads, may wish to consider implementing a {@link Callback} trigger instead.
   */
  interface Direct extends RetrieveTrigger {
    IntermediateResult retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options);

    IntermediateResult[] retrieve(SSNamespaceStore nsStore, DHTKey[] keys, SSRetrievalOptions options);
  }

  public Iterator<DHTKey> keyIterator();

  public long getTotalKeys();

  public boolean subsumesStorage();
}
