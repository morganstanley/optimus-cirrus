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
package optimus.graph.loom;

import static optimus.graph.DiagnosticSettings.lCompilerAssumeGlobalMutation;
import static optimus.graph.DiagnosticSettings.lCompilerDebug;
import static optimus.graph.DiagnosticSettings.lCompilerLevel;
import static optimus.graph.DiagnosticSettings.lCompilerEnqueueEarlier;
import static optimus.graph.DiagnosticSettings.lCompilerQueueSizeSensitive;
import static optimus.graph.DiagnosticSettings.lCompilerSkipFoldingBlocks;
import static optimus.graph.loom.LoomConfig.CompilerAssumeGlobalMutationParam;
import static optimus.graph.loom.LoomConfig.CompilerDebugParam;
import static optimus.graph.loom.LoomConfig.CompilerLevelParam;
import static optimus.graph.loom.LoomConfig.CompilerEnqueueEarlierParam;
import static optimus.graph.loom.LoomConfig.CompilerQueueSizeSensitiveParam;
import static optimus.graph.loom.LoomConfig.CompilerRetainModifiedByteCodeParam;
import org.objectweb.asm.tree.AnnotationNode;

public class CompilerArgs {
  public int level;
  public boolean debug;

  public boolean skipFoldingBlocks; // disables BlockFolding phase

  // uses postOrder from DOMCalculator, rather than the original parsing order
  public boolean usePostOrder;
  public boolean enqueueEarlier;
  public boolean queueSizeSensitive;
  public boolean assumeGlobalMutation;

  public boolean retainModifiedByteCode;

  public CompilerArgs(
      int level,
      boolean debug,
      boolean enqueueEarlier,
      boolean queueSizeSensitive,
      boolean assumeGlobalMutation,
      boolean skipFoldingBlocks) {
    this.level = level;
    this.debug = debug;
    this.enqueueEarlier = enqueueEarlier;
    this.queueSizeSensitive = queueSizeSensitive;
    this.assumeGlobalMutation = assumeGlobalMutation;
    this.skipFoldingBlocks = skipFoldingBlocks;
  }

  public CompilerArgs(CompilerArgs other) {
    this.level = other.level;
    this.debug = other.debug;
    this.skipFoldingBlocks = other.skipFoldingBlocks;
    this.usePostOrder = other.usePostOrder;
    this.enqueueEarlier = other.enqueueEarlier;
    this.queueSizeSensitive = other.queueSizeSensitive;
    this.assumeGlobalMutation = other.assumeGlobalMutation;
    this.retainModifiedByteCode = other.retainModifiedByteCode;
  }

  public static final CompilerArgs Default =
      new CompilerArgs(
          lCompilerLevel,
          lCompilerDebug,
          lCompilerEnqueueEarlier,
          lCompilerQueueSizeSensitive,
          lCompilerAssumeGlobalMutation,
          lCompilerSkipFoldingBlocks);

  public static CompilerArgs parse(AnnotationNode ann, CompilerArgs defaultCArgs) {
    var values = ann.values;
    if (values != null) {
      var cArgs = new CompilerArgs(defaultCArgs);
      for (int i = 0, n = values.size(); i < n; i += 2) {
        var name = (String) values.get(i);
        var value = values.get(i + 1);
        switch (name) {
          case CompilerLevelParam:
            cArgs.level = (int) value;
            break;
          case CompilerDebugParam:
            cArgs.debug = (boolean) value;
            break;
          case CompilerEnqueueEarlierParam:
            cArgs.enqueueEarlier = (boolean) value;
            break;
          case CompilerQueueSizeSensitiveParam:
            cArgs.queueSizeSensitive = (boolean) value;
            break;
          case CompilerAssumeGlobalMutationParam:
            cArgs.assumeGlobalMutation = (boolean) value;
          case CompilerRetainModifiedByteCodeParam:
            cArgs.retainModifiedByteCode = (boolean) value;
        }
      }
      return cArgs;
    }
    return defaultCArgs;
  }
}
