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
package optimus.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

public class SystemFinalization {
  private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

  // TODO (OPTIMUS-63036): finalizers are deprecated by Java 18 for removal in a future version.
  // We'll need to find another solution, e.g. an automatic rewrite to use the Cleaner pattern.
  // This method can then be rewritten to drain whatever queue we have in that new solution.
  public static void runFinalizers() {
    System.runFinalization();
  }

  public static int getObjectPendingFinalizationCount() {
    return memoryBean.getObjectPendingFinalizationCount();
  }
}
