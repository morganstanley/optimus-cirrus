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
package com.ms.silverking.cloud.dht.collection.test;

import com.ms.silverking.cloud.dht.collection.IntArrayDHTKeyCuckoo;
import com.ms.silverking.collection.cuckoo.TableFullException;
import com.ms.silverking.collection.cuckoo.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntRehashTest {
  private static final int totalEntries = 16;
  private static final int numSubTables = 4;
  private static final int entriesPerBucket = 4;
  private static final int cuckooLimit = 32;

  private static Logger log = LoggerFactory.getLogger(IntRehashTest.class);

  public static void rehashTest(int size) {
    IntArrayDHTKeyCuckoo map;

    map =
        new IntArrayDHTKeyCuckoo(
            new WritableCuckooConfig(totalEntries, numSubTables, entriesPerBucket, cuckooLimit));
    for (int i = 0; i < size; i++) {
      try {
        map.put(new SimpleKey(0, i), i);
      } catch (TableFullException tfe) {
        log.info("rehashing");
        map = IntArrayDHTKeyCuckoo.rehashAndAdd(map, new SimpleKey(0, i), i);
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      int size;

      if (args.length != 1) {
        log.error("args: <size>");
        return;
      }
      size = Integer.parseInt(args[0]);
      rehashTest(size);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
