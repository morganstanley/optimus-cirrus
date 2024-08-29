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
package optimus.dht.common.util.monitoring.gc;

import java.lang.management.MemoryUsage;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCMemoryPoolSizes {

  private static final Logger logger = LoggerFactory.getLogger(GCMemoryPoolSizes.class);

  private static final MemoryUsage MEMORY_USAGE_ZERO = new MemoryUsage(0, 0, 0, 0);

  private final MemoryUsage eden;
  private final MemoryUsage survivor;
  private final MemoryUsage oldGen;
  private final MemoryUsage metaspace;
  private final MemoryUsage classSpace;
  private final MemoryUsage codeCache;

  public GCMemoryPoolSizes(
      MemoryUsage eden,
      MemoryUsage survivor,
      MemoryUsage oldGen,
      MemoryUsage metaspace,
      MemoryUsage classSpace,
      MemoryUsage codeCache) {
    this.eden = eden;
    this.survivor = survivor;
    this.oldGen = oldGen;
    this.metaspace = metaspace;
    this.classSpace = classSpace;
    this.codeCache = codeCache;
  }

  public static GCMemoryPoolSizes fromMap(Map<String, MemoryUsage> map) {
    MemoryUsage eden = MEMORY_USAGE_ZERO;
    MemoryUsage survivor = MEMORY_USAGE_ZERO;
    MemoryUsage oldGen = MEMORY_USAGE_ZERO;
    MemoryUsage metaspace = MEMORY_USAGE_ZERO;
    MemoryUsage classSpace = MEMORY_USAGE_ZERO;
    MemoryUsage codeCache = MEMORY_USAGE_ZERO;

    for (Map.Entry<String, MemoryUsage> entry : map.entrySet()) {

      String key = entry.getKey();
      MemoryUsage value = entry.getValue();

      if (key.contains("Eden")) {
        eden = value;
      } else if (key.contains("Survivor")) {
        survivor = value;
      } else if (key.contains("Old Gen")) {
        oldGen = value;
      } else if (key.contains("Shenandoah")) {
        oldGen = value;
      } else if (key.contains("ZHeap")) {
        oldGen = value;
      } else if (key.contains("ZGC Young Generation")) {
        eden = new MemoryUsage(value.getInit(), value.getUsed(), value.getCommitted(), -1);
      } else if (key.equals("Metaspace")) {
        metaspace = value;
      } else if (key.equals("Compressed Class Space")) {
        classSpace = value;
      } else if (key.equals("Code Cache")) {
        codeCache = value;
      } else if (key.startsWith("CodeHeap")) {
        if (codeCache == null) {
          codeCache = value;
        } else {
          codeCache =
              new MemoryUsage(
                  codeCache.getInit() + value.getInit(),
                  codeCache.getUsed() + value.getUsed(),
                  codeCache.getCommitted() + value.getCommitted(),
                  codeCache.getMax() + value.getMax());
        }
      } else {
        logger.warn("Unknown memory pool name: {}", key);
      }
    }

    return new GCMemoryPoolSizes(eden, survivor, oldGen, metaspace, classSpace, codeCache);
  }

  public MemoryUsage getEden() {
    return eden;
  }

  public MemoryUsage getSurvivor() {
    return survivor;
  }

  public MemoryUsage getOldGen() {
    return oldGen;
  }

  public MemoryUsage getMetaspace() {
    return metaspace;
  }

  public MemoryUsage getClassSpace() {
    return classSpace;
  }

  public MemoryUsage getCodeCache() {
    return codeCache;
  }

  public long getHeapUsed() {
    return eden.getUsed() + survivor.getUsed() + oldGen.getUsed();
  }

  public long getHeapMax() {
    return Math.max(eden.getMax(), 0)
        + Math.max(survivor.getMax(), 0)
        + Math.max(oldGen.getMax(), 0);
  }
}
