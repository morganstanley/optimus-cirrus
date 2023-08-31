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
package com.ms.silverking.thread.lwt;

import com.ms.silverking.collection.LightLinkedBlockingQueue;

public class LWTPoolParameters {
  private final String name;
  private final int targetSize;
  private final int maxSize;
  private final LightLinkedBlockingQueue<AssignedWork> commonQueue;
  private final int workUnit;

  private static final int defaultTargetSize = 1;
  private static final int defaultMaxSize = 1;
  private static final LightLinkedBlockingQueue<AssignedWork> defaultCommonQueue = null;
  private static final int defaultWorkUnit = 1;

  private LWTPoolParameters(
      String name,
      int targetSize,
      int maxSize,
      LightLinkedBlockingQueue<AssignedWork> commonQueue,
      int workUnit) {
    this.name = name;
    this.targetSize = targetSize;
    this.maxSize = maxSize;
    this.commonQueue = commonQueue;
    this.workUnit = workUnit;
  }

  public static LWTPoolParameters create(String name) {
    return new LWTPoolParameters(
        name, defaultTargetSize, defaultMaxSize, defaultCommonQueue, defaultWorkUnit);
  }

  public LWTPoolParameters targetSize(int targetSize) {
    return new LWTPoolParameters(
        name, targetSize, Math.max(maxSize, targetSize), commonQueue, workUnit);
  }

  public LWTPoolParameters maxSize(int maxSize) {
    return new LWTPoolParameters(name, targetSize, maxSize, commonQueue, workUnit);
  }

  public LWTPoolParameters commonQueue(LightLinkedBlockingQueue<AssignedWork> commonQueue) {
    return new LWTPoolParameters(name, targetSize, maxSize, commonQueue, workUnit);
  }

  public LWTPoolParameters workUnit(int workUnit) {
    return new LWTPoolParameters(name, targetSize, maxSize, commonQueue, workUnit);
  }

  public String getName() {
    return name;
  }

  public int getTargetSize() {
    return targetSize;
  }

  public int getMaxSize() {
    return maxSize;
  }

  public LightLinkedBlockingQueue<AssignedWork> getCommonQueue() {
    return commonQueue;
  }

  public int getWorkUnit() {
    return workUnit;
  }

  @Override
  public String toString() {
    return name + ":" + targetSize + ":" + maxSize + ":" + commonQueue + ":" + workUnit;
  }
}
