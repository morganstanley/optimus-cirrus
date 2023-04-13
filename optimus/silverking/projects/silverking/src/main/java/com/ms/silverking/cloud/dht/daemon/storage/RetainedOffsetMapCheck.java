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
package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetainedOffsetMapCheck implements EntryRetentionCheck {
  private final Set<Integer> retainedOffsets;
  private final Set<Integer> discardedOffsets;

  private static Logger log = LoggerFactory.getLogger(RetainedOffsetMapCheck.class);

  public RetainedOffsetMapCheck(Set<Integer> retainedOffsets, Set<Integer> discardedOffsets) {
    this.retainedOffsets = retainedOffsets;
    this.discardedOffsets = discardedOffsets;
  }

  @Override
  public boolean shouldRetain(int segmentNumber, DataSegmentWalkEntry entry) {
    int offset;

    offset = entry.getOffset();
    if (retainedOffsets.contains(offset)) {
      return true;
    } else if (discardedOffsets.contains(offset)) {
      return false;
    } else {
      log.info("Unexpected unknown offset in RetainedOffsetMapCheck.shouldRetain() {} {}", entry.getKey(), offset);
      return true;
    }
  }
}
