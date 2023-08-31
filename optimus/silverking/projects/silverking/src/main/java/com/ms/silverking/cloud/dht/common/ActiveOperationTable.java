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
package com.ms.silverking.cloud.dht.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.id.UUIDBase;

/**
 * Used by both client and server to prevent duplicate operation sends.
 *
 * <p>Currently, this is only applied to receive operations
 */
public class ActiveOperationTable<R extends Retrieval> {
  // private final ConcurrentMap<DHTKey,RetrievalGroup> activeRetrievals;
  private final ConcurrentMap<UUIDBase, Retrieval> activeRetrievals;
  // private final ConcurrentMap<DHTKey,RetrievalGroup> activeRetrievals;

  // FUTURE - RetrievalGroup code is currently disabled. Think about whether or
  // not we want to add it back in.

  public ActiveOperationTable() {
    // activeRetrievals = new MapMaker().weakKeys().concurrencyLevel(2).makeMap();
    // FUTURE - think about cleanup and how it will work for clients and proxy servers
    activeRetrievals = new ConcurrentHashMap<>();
  }

  // public RetrievalGroup getRetrievalGroup(DHTKey key) {
  //    return activeRetrievals.get(key);
  // }

  public void addRetrieval(Retrieval retrieval) {
    /*
    int overlapped;

    overlapped = 0;
    for (DHTKey key : retrieval.getDHTKeys()) {
        RetrievalGroup  rGroup;
        boolean         overlapping;

        rGroup = activeRetrievals.get(key);
        if (rGroup == null) {
            RetrievalGroup  existingGroup;

            rGroup = new RetrievalGroup();
            existingGroup = activeRetrievals.putIfAbsent(key, rGroup);
            if (existingGroup != null) {
                rGroup = existingGroup;
            }
        }
        //System.out.println(key +"\t => \t"+ rGroup);
        overlapping = rGroup.addRetrieval(retrieval);
        if (overlapping) {
            overlapped++;
        }
    }
    System.out.printf("overlapped %d/%d\n", overlapped, retrieval.getDHTKeys().size());
    System.out.printf("activeRetrievals.size() %d\n", activeRetrievals.size());
    */
    activeRetrievals.put(retrieval.getUUID(), retrieval);
  }

  public void receivedRetrievalResponse(MessageGroup message) {
    throw new RuntimeException("legacy code invoked");
    /*
    Retrieval   retrieval;

    retrieval = activeRetrievals.get(message.getUUID());
    if (retrieval != null) {
        for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator
        (MessageGroupKVEntry.ValueMode.Local)) {
            retrieval.addResponse(entry);
        }
        if (retrieval.getState() != OperationState.INCOMPLETE) {
            activeRetrievals.remove(message.getUUID());
        }
    } else {
        Log.info("No active retrieval found for: ", message.getUUID());
    }
    */
    /*
    Log.fine("in receivedRetrievalResponse()");
    for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator()) {
        RetrievalGroup  rGroup;

        rGroup = activeRetrievals.get(entry);
        if (rGroup == null) {
            Log.info("No RetrievalGroup for ", entry);
        } else {
            rGroup.receivedResponse(entry);
            if (rGroup.isComplete()) {
                activeRetrievals.remove(entry);
            }
        }
    }
    Log.fine("out receivedRetrievalResponse()");
    */
  }
}
