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
package optimus.graph.diagnostics.trace;

import java.util.Arrays;

import optimus.graph.NodeTask;
import optimus.graph.PropertyNode;
import optimus.graph.diagnostics.PNodeTask;
import optimus.graph.diagnostics.messages.ProfilerEventsWriter;
import optimus.platform.storable.Entity;

class OGEventObserverUtils {

  static void writeHashes(ProfilerEventsWriter trace, PNodeTask pnt, NodeTask task) {
    int tid = task.getId();
    int pid = task.getProfileId(); // aka [P]NodeTaskInfo
    Object[] args = pnt != null ? pnt.args() : task.args();

    int ssHash = getScenarioStackHash(task);
    int entityHash = getEntityHash(task);
    int argsHash = getArgsHash(args);
    int resultHash = getResultsHash(task);
    int ssIdHash = getScenarioStackIdentityHash(task);
    int entityIdHash = getEntityIdentityHash(task);
    int argsIdHash = getArgsIdentityHash(args);
    int resultIdHash = getResultsIdentityHash(task);
    trace.summaryHashes(
        tid,
        pid,
        ssHash,
        entityHash,
        argsHash,
        resultHash,
        ssIdHash,
        entityIdHash,
        argsIdHash,
        resultIdHash);
  }

  private static int getScenarioStackHash(NodeTask task) {
    // TODO (OPTIMUS-59089): This should be replaced by _cacheID
    return task.scenarioStack().hashCode();
  }

  private static int getScenarioStackIdentityHash(NodeTask task) {
    return System.identityHashCode(task.scenarioStack());
  }

  private static int getEntityHash(NodeTask task) {
    if (task instanceof PropertyNode) {
      Entity entity = ((PropertyNode) task).entity();
      if (entity != null) return entity.hashCode();
    }
    return 0;
  }

  private static int getEntityIdentityHash(NodeTask task) {
    if (task instanceof PropertyNode) {
      Object entity = ((PropertyNode) task).entity();
      if (entity != null) return System.identityHashCode(entity);
    }
    return 0;
  }

  private static int getArgsHash(Object[] args) {
    if (args == null || args.length == 0) return 0;
    return Arrays.hashCode(args);
  }

  private static int getArgsIdentityHash(Object[] args) {
    if (args == null || args.length == 0) return 0;
    int hashCode = 0;
    for (Object arg : args) {
      hashCode *= 31;
      if (arg == null) continue;
      if (arg.getClass().isPrimitive()) {
        hashCode += arg.hashCode();
      } else {
        hashCode += System.identityHashCode(arg);
      }
    }
    return hashCode;
  }

  private static int getResultsHash(NodeTask task) {
    // n.b. at the point when this method is called (i.e. during completion), result is set but the
    // node is not yet
    // marked complete, so we use this special method to access the result
    Object result = task.resultObjectEvenIfIncomplete();
    if (result != null) return result.hashCode();
    return 0;
  }

  private static int getResultsIdentityHash(NodeTask task) {
    // n.b. at the point when this method is called (i.e. during completion), result is set but the
    // node is not yet
    // marked complete, so we use this special method to access the result
    Object result = task.resultObjectEvenIfIncomplete();
    if (result != null) return System.identityHashCode(result);
    return 0;
  }
}
