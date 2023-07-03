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
package optimus.deps;

import java.util.concurrent.atomic.AtomicLong;

public class TransformationStatistics {
  public final AtomicLong ignored = new AtomicLong(0);
  public final AtomicLong unchanged = new AtomicLong(0);
  public final AtomicLong visited = new AtomicLong(0);
  public final AtomicLong instrumented = new AtomicLong(0);
  public final AtomicLong optimusClasses = new AtomicLong(0);
  public final AtomicLong failures = new AtomicLong(0);

  @Override
  public String toString() {
    return "TransformationStatistics{ignored="
        + ignored
        + ", unchanged="
        + unchanged
        + ", visited="
        + visited
        + ", instrumented="
        + instrumented
        + ", optimusClasses="
        + optimusClasses
        + ", failures="
        + failures
        + '}';
  }
}
