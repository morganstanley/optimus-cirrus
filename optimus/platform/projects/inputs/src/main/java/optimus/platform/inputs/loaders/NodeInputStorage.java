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
package optimus.platform.inputs.loaders;

import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.NodeInputResolver;
import optimus.platform.inputs.NodeInputs;

/**
 * Accumulates {@link NodeInput} and their respective bound values when loading from different
 * sources (ie: system properties / environment variables / etc ...)
 *
 * <p>The expected behaviour when adding a local input binding to the {@link NodeInputStorage} is as
 * follows:
 *
 * <ul>
 *   <li>if there is an existing engine specific binding for the same {@link NodeInput}, the newly
 *       added binding is <b>always</b> marked as non-forwarding
 *   <li>if instead there is an existing local input binding for the same {@link NodeInput}, the
 *       binding's forwarding behaviour is preserved (eg: if the {@link NodeInput} was loaded as
 *       not-forwarding to engine, it will be kept non-forwarding)
 *   <li>finally, if there is no existing binding for the same {@link NodeInput}, the boolean flag
 *       in {@link NodeInputStorage#appendLocalInput(NodeInput, Object, boolean)} determines the
 *       binding's forwarding behaviour (except when {@link NodeInput#engineForwardingBehavior ()}
 *       is true, in which case the binding is assumed to be forwarding)
 * </ul>
 *
 * Please note:
 *
 * <ul>
 *   <li>if the same {@link NodeInput} is loaded, with the same value, as both local and engine
 *       specific input, the two bindings will be collapsed into a local forwarding input
 *   <li>if the same {@link NodeInput} is loaded, with different values, as both local and engine
 *       specific input, the engine specific input will be forwarded during distribution, and the
 *       local input will be marked as non-forwarding
 * </ul>
 *
 * @param <N> the type of the underlying storage mechanism
 * @see Loaders
 * @implNote In Optimus we want to further specialize the behaviour of the returned {@link
 *     NodeInputResolver}, to account for global (affects whole process) and scoped node inputs
 *     (live on scenario stack). Nothing of this makes sense when running in Interop, and Java does
 *     not support higher kinded types, so this extra wrapping interface is needed.
 */
public abstract class NodeInputStorage<N> {

  /**
   * @return the underlying storage mechanism
   */
  public abstract N underlying();

  /**
   * Adds a {@link NodeInput} and its corresponding parameter to the underlying storage.
   *
   * @param input the {@link NodeInput} to append to the underlying storage
   * @param value the value to be bound to the {@link NodeInput}
   * @param forwardToEngine whether the value should be forwarded to engines during distribution
   */
  abstract <T> NodeInputStorage<N> appendLocalInput(
      NodeInput<T> input, T value, boolean forwardToEngine, LoaderSource source);

  /**
   * @see #appendLocalInput(NodeInput, Object, boolean)
   */
  final <T> NodeInputStorage<N> appendLocalInput(
      NodeInputResolver.Entry<T> entry, boolean forwardToEngine) {
    return appendLocalInput(
        entry.nodeInput(),
        entry.sourcedInput().value(),
        forwardToEngine,
        entry.sourcedInput().source());
  }

  static final void checkAppendToEngine(NodeInput<?> ni) {
    if (NodeInputs.isNeverForwarding(ni))
      throw new IllegalArgumentException(
          "Can't append an input to engine inputs that will never be forwarded");
  }

  /**
   * Adds a {@link NodeInput} and its corresponding parameter to the underlying storage. {@link
   * NodeInput} will only be visible on engines after distribution.
   *
   * @param input the {@link NodeInput} to append to the underlying storage
   * @param value the value to be bound to the {@link NodeInput}
   * @apiNote will crash if you try to pass in a {@link NodeInput} that {@link
   *     EngineForwarding.BEHAVIOR.NEVER} forwards
   */
  abstract <T> NodeInputStorage<N> appendEngineSpecificInput(
      NodeInput<T> input, T value, LoaderSource source);

  /**
   * @see #appendEngineSpecificInput(NodeInput, Object)
   */
  final <T> NodeInputStorage<N> appendEngineSpecificInput(NodeInputResolver.Entry<T> entry) {
    return appendEngineSpecificInput(
        entry.nodeInput(), entry.sourcedInput().value(), entry.sourcedInput().source());
  }
}
