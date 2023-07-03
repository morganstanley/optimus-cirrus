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
package optimus.graph.chaos;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Records the className#methodName of each method which calls the ChaosMonkeyRuntime hook. Useful
 * for asserting that the Chaos infrastructure is configured properly.
 */
public class RecordingMonkey implements ChaosMonkeyRuntime.ChaosMonkey {
  public final Set<String> recordedMethods = Collections.synchronizedSet(new HashSet<String>());

  public void doChaos() {
    StackTraceElement caller = Thread.currentThread().getStackTrace()[3];
    recordedMethods.add(caller.getClassName() + "#" + caller.getMethodName());
  }
}
