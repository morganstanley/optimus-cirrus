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

import java.util.ArrayList;
import java.util.function.Function;
import optimus.platform.util.PrettyStringBuilder;

public class NodeTaskVisitor {
  public ArrayList<NodeTask> path;

  /** Return true to continue walk, false to stop */
  protected boolean visit(NodeTask task, int depth) {
    return true; // Continue the walk
  }

  protected void visit(NodeCauseInfo info, int depth) {}

  protected boolean stopOnAnyFullStack() {
    return false;
  }

  protected boolean includeProxies() {
    return DiagnosticSettings.proxyInWaitChain;
  }

  protected boolean useLauncher() {
    return DiagnosticSettings.awaitStacks;
  }

  void reportCycle() {}

  public void visitEnd() {}

  public static class CommonVisitor extends NodeTaskVisitor {
    final boolean enableDangerousParamPrinting;
    final PrettyStringBuilder sb;
    final boolean fullNodeStack;
    ArrayList<NodeTask> list;

    ArrayList<NodeAwaiter> waitersToPrint;
    ArrayList<Integer> depths;
    Function<NodeTask, Boolean> consumeAndContinue;

    int expectedDepth = 0;
    int maxDepth = 0;
    int paths = 1;
    int nodes = 0;
    int maxLength;

    boolean skipRest = false;

    CommonVisitor() {
      this(false, new PrettyStringBuilder(), null, null, false, 1000);
    }

    CommonVisitor(
        boolean enableDangerousParamPrinting,
        PrettyStringBuilder sb,
        ArrayList<NodeTask> list,
        Function<NodeTask, Boolean> consumeAndContinue,
        boolean fullNodeStack,
        int maxLength) {
      this.maxLength = maxLength;
      this.enableDangerousParamPrinting = enableDangerousParamPrinting;
      this.sb = sb;
      this.list = list;
      this.consumeAndContinue = consumeAndContinue;
      this.fullNodeStack = fullNodeStack;
      if (sb != null) {
        this.waitersToPrint = new ArrayList<>();
        this.depths = new ArrayList<>();
      }
    }

    @Override
    public boolean visit(NodeTask task, int depth) {
      if (expectedDepth != depth) {
        paths++;
        skipRest = true;
      }
      expectedDepth = depth + 1;
      if (depth > maxDepth) maxDepth = depth;

      if (!fullNodeStack && skipRest) return true;

      nodes++;
      if (maxLength > 0 && nodes > maxLength) return false;

      if (list != null) list.add(task);

      if (sb != null) {
        waitersToPrint.add(task);
        depths.add(depth);
      }

      if (consumeAndContinue != null) return consumeAndContinue.apply(task);
      return true; // Continue
    }

    @Override
    protected void visit(NodeCauseInfo info, int depth) {
      if (!skipRest) {
        if (sb != null) {
          var details = info.details();
          if (!details.isEmpty()) {
            waitersToPrint.add(info);
            depths.add(depth);
          }
        }
      }
    }

    @Override
    public void visitEnd() {
      if (sb == null) return;
      for (int i = 0; i < waitersToPrint.size(); i++) {
        var waiter = waitersToPrint.get(i);
        if (waiter instanceof NodeTask task) addToOutput(task, depths.get(i));
        else if (waiter instanceof NodeCauseInfo nci) addToOutput(nci, depths.get(i));
      }
    }

    protected void addToOutput(NodeTask task, int depth) {
      var pluginTags = task.scenarioStack().pluginTags().iterator();

      String postfix = "";

      while (pluginTags.hasNext()) {
        Object pluginInfo = pluginTags.next().value();
        if (pluginInfo instanceof PluginNodeTraceInfo info) {
          if (info.node() == task) {
            postfix = " :: " + info.info();
            break;
          }
        }
      }
      postfix = postfix.replace(System.lineSeparator(), System.lineSeparator() + '\t');

      sb.append(String.format("%3d. ", depth));
      sb.appendln(task.stackLine(false) + task.stateAsString() + postfix);

      if (enableDangerousParamPrinting) {
        Object[] args = task.args();
        if (args != null) {
          for (Object arg : args) {
            sb.append("      ");
            sb.appendln(arg);
          }
        }
      }
    }

    protected void addToOutput(NodeCauseInfo nci, int depth) {
      sb.indent();
      sb.appendln(nci.details());
      sb.unIndent();
    }
  }
}
