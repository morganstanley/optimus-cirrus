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
package com.ms.silverking.time;

/**
 * A simple concrete implementation of StopwatchBase that utilizes a
 * RelNanosTimeSource and performs some basic quick checks.
 */
public class SimpleStopwatch extends StopwatchBase {
  private final RelNanosTimeSource relNanosTimeSource;

  protected SimpleStopwatch(RelNanosTimeSource relNanosTimeSource, long startTimeNanos) {
    super(startTimeNanos);
    this.relNanosTimeSource = relNanosTimeSource;
  }

  public SimpleStopwatch(RelNanosTimeSource relNanosTimeSource) {
    this(relNanosTimeSource, relNanosTimeSource.relTimeNanos());
  }

  public SimpleStopwatch() {
    this(SystemTimeSource.instance);
  }

  protected final long relTimeNanos() {
    return relNanosTimeSource.relTimeNanos();
  }

  // control

  @Override
  public void start() {
    ensureState(State.stopped);
    super.start();
  }

  @Override
  public void stop() {
    ensureState(State.running);
    super.stop();
  }

  // elapsed

  @Override
  public long getElapsedNanos() {
    ensureState(State.stopped);
    return super.getElapsedNanos();
  }
}
