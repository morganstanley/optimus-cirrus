// UUIDBase.java

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
package com.ms.silverking.id;

import java.io.Serializable;
import java.util.UUID;

public class UUIDBase implements Comparable<UUIDBase>, Serializable {
  protected final UUID uuid;

  private static final ThreadLocal<ThreadUUIDState> tlThreadUUIDState;
  private static final boolean useIDThread = false;

  //private static final ThreadUUIDState commonUUIDState = new ThreadUUIDState();

  static {
    tlThreadUUIDState = new ThreadLocal<>();
  }

  private ThreadUUIDState getThreadUUIDState() {
    ThreadUUIDState state;

    state = tlThreadUUIDState.get();
    if (state == null) {
      state = new ThreadUUIDState();
      tlThreadUUIDState.set(state);
    }
    return state;
  }

  public UUIDBase() {
    //this.uuid = UUID.randomUUID();
    // Below is ~100x faster than above.
    // Also above grabs a global lock which is trouble when we have lots
    // of threads generating uuids.
    ThreadUUIDState state;

    if (useIDThread) {
      state = ((IDThread) Thread.currentThread()).getThreadUUIDState();
    } else {
      state = getThreadUUIDState();
    }
    this.uuid = new UUID(state.longMSB, state.getNextLongLSB());
    //this.uuid = new UUID(commonUUIDState.longMSB, commonUUIDState.getNextLongLSB());
  }

  public UUIDBase(boolean random) {
    if (random) {
      this.uuid = UUID.randomUUID();
    } else {
      // Below is ~100x faster than above.
      // Also above grabs a global lock which is trouble when we have lots
      // of threads generating uuids.
      ThreadUUIDState state;

      if (useIDThread) {
        state = ((IDThread) Thread.currentThread()).getThreadUUIDState();
      } else {
        state = getThreadUUIDState();
      }
      this.uuid = new UUID(state.longMSB, state.getNextLongLSB());
    }
  }

  public static UUIDBase random() {
    return new UUIDBase(true);
  }

  protected UUIDBase(UUID uuid) {
    this.uuid = uuid;
  }

  public UUIDBase(long msb, long lsb) {
    this.uuid = new UUID(msb, lsb);
  }

  public int compareTo(UUIDBase otherUUID) {
    return uuid.compareTo(otherUUID.uuid);
  }

  public long getMostSignificantBits() {
    return uuid.getMostSignificantBits();
  }

  public long getLeastSignificantBits() {
    return uuid.getLeastSignificantBits();
  }

  public UUID getUUID() {
    return uuid;
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    UUIDBase otherUUID;

    otherUUID = (UUIDBase) other;
    return (otherUUID == this) || (otherUUID != null && uuid.equals(otherUUID.uuid));
  }

  @Override
  public String toString() {
    return uuid.toString();
  }

  public static UUIDBase fromString(String def) {
    return new UUIDBase(UUID.fromString(def));
  }
}
