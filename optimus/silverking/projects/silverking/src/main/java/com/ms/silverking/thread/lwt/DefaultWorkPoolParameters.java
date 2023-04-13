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

public class DefaultWorkPoolParameters {
  private final int numConcurrentThreads;
  private final int numNonConcurrentThreads;
  private final int maxConcurrentThreads;
  private final int maxNonConcurrentThreads;
  private final int workUnit;
  private final boolean ignoreDoubleInit;

  private static final int defaultWorkUnit = 1;
  private static final boolean defaultIgnoreDoubleInit = false;
  private static final double defaultExtraThreadFactor = 1.0;
  private static final double defaultMaxThreadFactor = 4.0;

  private DefaultWorkPoolParameters(int numConcurrentThreads, int numNonConcurrentThreads, int maxConcurrentThreads,
      int maxNonConcurrentThreads, int workUnit, boolean ignoreDoubleInit) {
    this.numConcurrentThreads = numConcurrentThreads;
    this.numNonConcurrentThreads = numNonConcurrentThreads;
    this.maxConcurrentThreads = maxConcurrentThreads;
    this.maxNonConcurrentThreads = maxNonConcurrentThreads;
    this.workUnit = workUnit;
    this.ignoreDoubleInit = ignoreDoubleInit;
  }

  public static DefaultWorkPoolParameters defaultParameters() {
    int numThreads;
    int maxThreads;

    numThreads = extraThreadFactorToNumThreads(defaultExtraThreadFactor);
    maxThreads = extraThreadFactorToNumThreads(defaultMaxThreadFactor);
    return new DefaultWorkPoolParameters(numThreads, 0, maxThreads, 0, defaultWorkUnit, defaultIgnoreDoubleInit);
  }

  private static int extraThreadFactorToNumThreads(double extraThreadFactor) {
    return (int) ((double) LWTConstants.numProcessors * extraThreadFactor);
  }

  public DefaultWorkPoolParameters workUnit(int workUnit) {
    return new DefaultWorkPoolParameters(numConcurrentThreads, numNonConcurrentThreads, maxConcurrentThreads,
        maxNonConcurrentThreads, workUnit, ignoreDoubleInit);
  }

  public DefaultWorkPoolParameters ignoreDoubleInit(boolean ignoreDoubleInit) {
    return new DefaultWorkPoolParameters(numConcurrentThreads, numNonConcurrentThreads, maxConcurrentThreads,
        maxNonConcurrentThreads, workUnit, ignoreDoubleInit);
  }

  public int getNumConcurrentThreads() {
    return numConcurrentThreads;
  }

  public int getNumNonConcurrentThreads() {
    return numNonConcurrentThreads;
  }

  public int getMaxConcurrentThreads() {
    return maxConcurrentThreads;
  }

  public int getMaxNonConcurrentThreads() {
    return maxNonConcurrentThreads;
  }

  public int getWorkUnit() {
    return workUnit;
  }

  public boolean getIgnoreDoubleInit() {
    return ignoreDoubleInit;
  }

  @Override
  public String toString() {
    return numConcurrentThreads + ":" + numNonConcurrentThreads + ":" + workUnit + ":" + ignoreDoubleInit;
  }
}
