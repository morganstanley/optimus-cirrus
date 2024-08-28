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
package optimus.export;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Annotation for parameters of entity's exported @node properties */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface xArg {
  /**
   * Specifies the argument name to be generated. If not specified, default used will be the
   * argument name as it is.
   */
  String name() default "";

  /**
   * Specifies the argument description to be generated. If not specified, generated description
   * will be "No description provided".
   */
  String desc() default "No description provided";

  /**
   * Specifies if excel plugin should invoke the specified functions if this argument refers to an
   * empty cell. In case of arrays or matrix, it refers to one of the cells in the range being
   * empty.
   *
   * <p>eg: if A1=1 and A2=empty, then the following will not cause server to be invoked if
   * preventEmptyArgs = true
   *
   * <p>=func1(A2), =func2(A1:A2), =func3(A1:A2)
   *
   * <p>The default is to call the function even if there are empty references.
   *
   * <p>This takes precedence over whatever is set in xFunc
   */
  TriState preventEmpty() default TriState.UNSET;

  /**
   * Similar to preventEmptyArgs but deals with cells having an Error like =NA() or =1/0
   *
   * <p>The default is to call the function even if there are errors.
   *
   * <p>This takes precedence over whatever is set in xFunc
   */
  TriState preventError() default TriState.UNSET;

  /**
   * Specifies if excel plugin should invoke the specified functions if this arguments is not
   * provided. This is not applicable to ranges of arrays or matrices because you can not represent
   * a missing value in a range.
   *
   * <p>eg: if func takes 3 parametes, then the following wont be called if preventMissingArgs =
   * true
   *
   * <p>=func(), =func(A1), =func(A1,B2), =func(A1,,C1)
   *
   * <p>The default is to call the function even if there are missing parameters.
   *
   * <p>This takes precedence over whatever is set in xFunc
   */
  TriState preventMissing() default TriState.UNSET;

  /**
   * This is a union of preventEmpty, preventError, and preventMissing. Generally users will set
   * just this argument to prevent all error conditions.
   *
   * <p>The default is false.
   *
   * <p>This takes precedence over whatever is set in xFunc
   */
  TriState preventInvalid() default TriState.UNSET;
}
