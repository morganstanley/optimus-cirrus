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
package optimus.platform;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * It tells entityplugin to just generate a minimal version of func$newNode, func$queued and not to
 * transform @nodeSync func. This also tells entityagent to transform the class into a loom calls,
 * which includes a proper implementation for func$newNode, func$queued and @nodeSync func and a new
 * method func$_.
 *
 * <p>Note: In the future where most everything is loom(ed) we can find ourselves in the situation
 * where all/most entity classes would have @loom attribute. As a small optimization we can check
 * for the base class first and not even emit the @loom attribute to save on some space and
 * checking...
 *
 * <p>[SEE_LOOM_STATIC_FUNC] We currently don't support static @node functions, which seems like we
 * shouldn't even care... Currently scala however automatically generates forwarders between modules
 * and class definitions We might want to support all kinds of additional permutations
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
public @interface loom {

  // used by entityplugin to track all calls to @nodeSync (but not @async!) functions
  @SuppressWarnings("unused")
  String[] nodes() default {};

  /**
   * List of immutable types (for example entities)
   * <li>Any class derived from Entity mentioned in class
   * <li>Any Type passed as an argument to node call and returned from a node call
   * <li>Consider adding support for IJ's @Contract
   * <li>This is on the top of the list in LCompiler
   */
  @SuppressWarnings("unused")
  String[] immutables() default {};

  // used by entityplugin to track all @nodeLift params (as they will be converted to anonymous
  // functions)
  @SuppressWarnings("unused")
  String[] lambdas() default {};

  // used by entityplugin to track column numbers of lambdas
  @SuppressWarnings("unused")
  int[] lcn() default {};
}
