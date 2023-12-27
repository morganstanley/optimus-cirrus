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
package optimus.platform.inputs;

/**
 * Class that defines the behavior for how node inputs should be serialized and in what cases they
 * should be forwarded to engines
 */
public class EngineForwarding {

  /**
   * Serialization behavior for inputs.
   *
   * <p>JAVA -> is serialized via normal Java serialization ENGINE_SPEC -> is serialized by us
   * outside of normal Java serialization so these inputs must require restart
   */
  public enum Serialization {
    JAVA,
    ENGINE_SPEC
  }

  /**
   * Forwarding behavior for inputs.
   *
   * <p>ALWAYS -> this input is always forwarded to engines no matter the context that it is added
   * DYNAMIC -> the forwarding behavior is based on the context, if added via loaders then it will
   * not forward if added via `withExtraInput` then it will be for example (can also be though of as
   * just not always and not never forwarding) NEVER -> input is never forwarded to engines no
   * matter what (see {@link NonForwardingPluginTagKey })
   *
   * @implNote currently if the behavior is NEVER then the serialization behavior for that input
   *     will never be read and can be left unimplemented. This is naturally because if we never
   *     forward it then it never needs to get serialized to get sent to any other machine
   */
  public enum Behavior {
    ALWAYS,
    NEVER,
    DYNAMIC
  }
}
