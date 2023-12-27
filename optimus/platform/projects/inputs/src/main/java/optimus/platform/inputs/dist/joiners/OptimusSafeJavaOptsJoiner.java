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
package optimus.platform.inputs.dist.joiners;

import java.util.StringJoiner;

import optimus.platform.inputs.EngineAwareNodeInputResolver;
import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.NodeInputResolver;
import optimus.platform.inputs.dist.GSFSections.SafeJavaOpt;
import optimus.platform.inputs.loaders.OptimusDistJavaOptsParser;

public class OptimusSafeJavaOptsJoiner {

  public static String joinSafeJavaOptsFromEnv() {
    return join(OptimusDistJavaOptsParser.parseFromEnv());
  }

  public static String join(EngineAwareNodeInputResolver resolver) {
    StringJoiner joiner = new StringJoiner(" ");
    resolver.engineForwardingForEach(
        (nodeInput, value) -> {
          SafeJavaOpt<Object> safeJavaOpt = nodeInput.gsfSection().asSafeJavaOpt();
          if (safeJavaOpt != null) {
            joiner.add(safeJavaOpt.invertAndFormat(value));
          }
        });
    String joined = joiner.toString();
    if (joined.isEmpty()) return null;
    return joined;
  }
}
