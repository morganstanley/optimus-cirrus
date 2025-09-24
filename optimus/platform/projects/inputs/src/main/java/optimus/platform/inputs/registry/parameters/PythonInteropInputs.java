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
package optimus.platform.inputs.registry.parameters;

import java.util.Collections;
import optimus.platform.inputs.EngineForwarding;
import optimus.platform.inputs.NodeInputs.ScopedSINodeInput;
import optimus.platform.inputs.NodeInputs;
import optimus.platform.inputs.dist.GSFSections;
import optimus.platform.inputs.registry.CombinationStrategies;
import optimus.platform.inputs.registry.Source;

public class PythonInteropInputs {

  public static final ScopedSINodeInput<String> TpaPath =
      new NodeInputs.BaseScopedSIInput<>(
          /* name = */ "TPA_PATH",
          /* description = */ "Path of .tpa file containing instructions for python venv creation",
          /* defaultValue = */ null,
          /* sources = */ Collections.singletonList(Source.fromEnvironmentVariable("TPA_PATH")),
          /* gsfSection = */ GSFSections.none(),
          /* requiresRestart = */ true,
          /* forwardingBehaviour = */ EngineForwarding.Behavior.ALWAYS,
          /* combinationStrategy = */ CombinationStrategies.distCombinator(),
          /* isTransient = */ false);
}
