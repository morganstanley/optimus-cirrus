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
package com.ms.silverking.cloud.dht;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;

/**
 * Time that value was created.
 */
public class CreationTime implements Comparable<CreationTime> {
  private final long creationTimeNanos;

  private static final SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss.SSS zzz yyyy");

  public CreationTime(long creationTimeNanos) {
    this.creationTimeNanos = creationTimeNanos;
  }

  public long inNanos() {
    return creationTimeNanos;
  }

  public long inMillis() {
    return SystemTimeUtil.systemTimeNanosToEpochMillis(creationTimeNanos);
  }

  @Override
  public int hashCode() {
    return (int) creationTimeNanos;
  }

  @Override
  public boolean equals(Object o) {
    CreationTime oct;

    oct = (CreationTime) o;
    return creationTimeNanos == oct.creationTimeNanos;
  }

  @Override
  public int compareTo(CreationTime o) {
    if (creationTimeNanos < o.creationTimeNanos) {
      return -1;
    } else if (creationTimeNanos > o.creationTimeNanos) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public String toString() {
    return Long.toString(creationTimeNanos);
  }

  public String toDateString() {
    return sdf.format(new Date(inMillis()));
  }
}
