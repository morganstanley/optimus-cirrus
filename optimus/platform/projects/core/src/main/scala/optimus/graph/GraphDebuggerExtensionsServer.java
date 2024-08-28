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
package optimus.graph;

import optimus.platform.debugger.OptimusCutPoint;
import optimus.core.MonitoringBreadcrumbs$;
import optimus.graph.profiled.NodeSync;

/** Code used in wait chain extension for Intellij Debugger */
public class GraphDebuggerExtensionsServer {

  // Used to load this class early (in NodeTask.java)
  public static String name = "GraphDebuggerExtensionsServer";

  private static volatile Boolean alreadySentCrumbs = false;

  /**
   * NOTE: Please do not remove this method before verifying that it is no longer used by
   * stratosphere code. [SEE_NODE_STACK_STR]
   */
  @SuppressWarnings("unused")
  public static String[] nodeAndJvmStackStrings() {
    if (!alreadySentCrumbs) {
      MonitoringBreadcrumbs$.MODULE$.sendAsyncStackTracesCrumb();
      alreadySentCrumbs = true;
    }
    return NodeStacks.reconstitutedNodeAndJvmStackAsStrings();
  }

  /**
   * NOTE: Please do not remove this method before verifying that it is no longer used by
   * stratosphere code. [SEE_NODE_STACK_ELEMS]
   */
  @SuppressWarnings("unused")
  public static StackElem[] nodeAndJvmOptimusStackElems() {
    return NodeStacks.reconstitutedNodeAndJvmStack();
  }

  /**
   * NOTE: Please do not remove this method before verifying that it is no longer used by
   * stratosphere code. [SEE_OPTIMUS_CUT_PTS]
   */
  @SuppressWarnings("unused")
  public static String[] cutPointsAsStrings() {
    // include here all the methods that we want to use to restart a node in the IJ debugger
    var cutPoints =
        new OptimusCutPoint[] {
          new OptimusCutPoint(PropertyNodeFSM.class.getName(), "run"),
          new OptimusCutPoint(NodeSync.class.getName(), "run")
        };

    var res = new String[cutPoints.length];

    for (int i = 0; i < cutPoints.length; i++) res[i] = OptimusCutPoint.write(cutPoints[i]);

    return res;
  }

  /**
   * NOTE: Please do not remove this method before verifying that it is no longer used by
   * stratosphere code. [SEE_IS_ASYNC_STACK_TRACES_ENABLED]
   */
  @SuppressWarnings("unused")
  public static boolean isAsyncStackTracesEnabledOnJvm() {
    return DiagnosticSettings.awaitStacks;
  }
}
