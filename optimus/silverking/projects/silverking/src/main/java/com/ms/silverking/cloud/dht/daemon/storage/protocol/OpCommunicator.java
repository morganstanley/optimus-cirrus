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
package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyedResult;
import com.ms.silverking.net.IPAndPort;

public class OpCommunicator<T extends DHTKey, R extends KeyedResult>
    implements OpVirtualCommunicator<T, R> {
  private volatile Map<IPAndPort, List<T>> replicaMessageLists;
  private volatile List<R> results;
  private Lock rmlLock;
  private Lock rLock;

  private static final boolean debug = false;

  protected static final int typicalReplication = 4; // FUTURE - could provide a real value
  protected static final int initialListSize = 8; // FUTURE - size the lists better

  OpCommunicator() {
    results = new ArrayList<>(initialListSize);
    rLock = new ReentrantLock();
    rmlLock = new ReentrantLock();
  }

  public Map<IPAndPort, List<T>> takeReplicaMessageLists() {
    Map<IPAndPort, List<T>> oldReplicaMessageLists;

    rmlLock.lock();
    try {
      oldReplicaMessageLists = replicaMessageLists;
      replicaMessageLists = null;
    } finally {
      rmlLock.unlock();
    }
    return oldReplicaMessageLists;
  }

  // must hold lock
  private Map<IPAndPort, List<T>> getReplicaMessageLists() {
    if (replicaMessageLists == null) {
      replicaMessageLists = new HashMap<>(typicalReplication);
    }
    return replicaMessageLists;
  }

  public List<R> takeResults() {
    List<R> oldResults;

    rLock.lock();
    try {
      oldResults = results;
      results = new ArrayList<>(initialListSize);
    } finally {
      rLock.unlock();
    }
    return oldResults;
  }

  @Override
  public void sendResult(R result) {
    if (debug) {
      System.out.printf("sendResult %s\n", result);
    }
    rLock.lock();
    try {
      results.add(result);
    } finally {
      rLock.unlock();
    }
    /*
    this.rString.add(result.toString());
    try {
        throw new RuntimeException();
    } catch (RuntimeException re) {
        this.re.add(re);
    }
    */
  }

  @Override
  public void forwardEntry(IPAndPort replica, T entry) {
    List<T> messageList;

    if (debug) {
      System.out.printf("forwardEntry %s %s\n", replica, entry);
    }
    assert replica != null;
    rmlLock.lock();
    try {
      messageList = getReplicaMessageLists().get(replica);
      if (messageList == null) {
        messageList = new ArrayList<>(initialListSize);
        replicaMessageLists.put(replica, messageList);
      }
      messageList.add(entry);
    } finally {
      rmlLock.unlock();
    }
  }

  /*
  private List<String> rString = new ArrayList<>();
  private List<RuntimeException> re = new ArrayList<>();
  public void displayDebug() {
      for (int i = 0; i < re.size(); i++) {
          System.out.println(rString.get(i));
          re.get(i).printStackTrace();
      }
  }
  */
}
