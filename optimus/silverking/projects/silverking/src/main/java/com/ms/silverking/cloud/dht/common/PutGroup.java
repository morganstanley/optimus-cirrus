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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Currently unused
 * FUTURE - Consider removing this class
 * <p>
 * For a single NS, key, lists all admitted puts.
 * Current plan, vary only by Version.
 */
public class PutGroup {
  private final List<Put> puts;
  private final Lock lock;

  // FUTURE - think about a non-locking, or usually non-locking implementation

  public PutGroup() {
    puts = new ArrayList<>();
    lock = new ReentrantLock();
  }

  // FUTURE - think about having a series of data model interfaces to control
  // behavior of this and other related classes

  // FUTURE - think about this
  private int indexOf(Version version) {
    long newV;

    newV = version.versionAsLong();
    for (int i = puts.size() - 1; i >= 0; i--) {
      long existingV;

      existingV = puts.get(i).getVersion().versionAsLong();
      if (existingV >= newV) {
        if (existingV == newV) {
          return i;
        } else {
          return -i - 1;
        }
      }
    }
    return -1;
  }

  public boolean addPut(Put put) {
    lock.lock();
    try {
      int index;

      index = indexOf(put.getVersion());
      if (index >= 0) {
        return false;
      } else {
        puts.add(-(index + 1), put);
        return true;
      }
    } finally {
      lock.unlock();
    }
  }

  public Put removePut(Version version) {
    lock.lock();
    try {
      int index;

      index = indexOf(version);
      if (index >= 0) {
        Put put;

        put = puts.get(index);
        puts.remove(index);
        return put;
      } else {
        return null;
      }
    } finally {
      lock.unlock();
    }
  }

  public void checkForExisting() {
    // probably pass in some method of sending this key on
    // need to reduce redundancy here
    // also need to combine for server/client
    //for (Retrieval retrieval : retrievals) {
    // if we still need this guy, then send him on
    //}
  }
}
