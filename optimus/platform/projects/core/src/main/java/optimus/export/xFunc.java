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

import optimus.excel.defaultsProvider;
import optimus.excel.xlfAutoFill;

/**
 * Annotation for entity's @node property that exports it to excel as a worksheet function and to
 * web browser as a JavaScript function
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface xFunc {
  /**
   * Specifies the excel worksheet or JavaScript function name to be generated. If not specified,
   * default used will be the entity property name as it is.
   */
  String name() default "";

  /**
   * Specifies the excel worksheet or JavaScript function description to be generated. If not
   * specified, excel function box will say "No description provided".
   */
  String desc() default "No description provided";

  /**
   * Specifies the excel worksheet function category to be applied to the generated function. If not
   * specified, function will be available under the default "Optimus" category.
   */
  String category() default "Optimus";

  /**
   * Specifies if the excel worksheet function to be generated is volatile or not. If not specified,
   * default is non-volatile.
   */
  boolean isVolatile() default false;

  /**
   * Specifies if the excel worksheet function to be generated is asynchronous or not. If not
   * specified, default is synchronous. Asynchronous behavior is achieved with the use of RTD server
   * running inside Optimus environment.
   */
  boolean isAsync() default false;

  /**
   * Specifies that if the excel worksheet or JavaScript function returns a priql query of type
   * Relation, this field can be set to true so that Optimus executes the query and returns final
   * result to excel. If not specified, default is false and Relation is returned to excel as a
   * regular object handle without executing. Also, since the result size is not known ahead of
   * time, autoFill (see below) will be set to true automatically when this field is set to true.
   */
  boolean isRelation() default false;

  /**
   * Specifies if the excel worksheet function to be generated should automatically fill adjacent
   * cells with values when result is an array. If not specified, default is disabled. If enabled,
   * then user doesn't need to pre-select a range of excel cells and hit CTRL + SHIFT + ENTER to get
   * the array result in excel, he can execute it regularly like a single cell function. But please
   * note that this behavior will override any adjacent cells with values from result array. So it
   * is your responsibility not to lose previous values by making sure result won't override any
   * other important cells.
   */
  xlfAutoFill autoFill() default @xlfAutoFill(enabled = false);

  /**
   * Specifies if the excel worksheet or JavaScript cell containing call for this node should be
   * automatically refreshed with latest value when tweaks are submitted and node value is modified
   * as a result. If not specified, default is false. It is recommended to set autoRefresh to true
   * on only those @nodes which are at the top of the dependency tree in excel. Subsequent dependent
   * cells in excel will be automatically evaluated by excel, so you don't need to set autoRefresh
   * on those.
   */
  boolean autoRefresh() default false;

  /**
   * Specifies if the excel call information (such as, whether it has been called as a regular
   * function or as an array formula) should be passed on to the actual @node as the last parameter
   * of type optimus.excel.XLCallInfo. If not specified, default is disabled.
   */
  boolean callInfo() default false;

  /**
   * Forces the result array to be returned as array instead of as a handle. Use case being
   * =someBuiltInExcelFunction(someOptimusFunctionWithAsArrayTrue(...)). In such a case, if asArray
   * is not set to true, array will be returned as a handle since the call is made from a single
   * cell as a regular formula. Setting asArray to true forces the actual array to be returned so
   * that it can be passed as an argument to the wrapper function
   */
  boolean asArray() default false;

  /**
   * Specifies a default return value provider in the case of any argument being an invalid handle.
   * If not specified, default is disabled. If enabled, allows to specify a default return value for
   * int, double, boolean or String return type functions. For object return type, default value
   * returned is empty object handle, represented as optimus.excel.Constants.EMPTY_HANDLE value
   * "[Empty]@".
   */
  defaultsProvider onInvalidHandle() default
      @defaultsProvider(enabled = true, generateError = true);

  /**
   * Specifies if excel plugin should invoke the specified functions if one of the arguments passed
   * to it refers to an empty cell. In case of arrays or matrix, it refers to one of the cells in
   * the range being empty.
   *
   * <p>eg: if A1=1 and A2=empty, then the following will not cause server to be invoked if
   * preventEmptyArgs = true
   *
   * <p>=func1(A2), =func2(A1:A2), =func3(A1:A2)
   *
   * <p>The default is to call the function even if there are empty references.
   *
   * <p>preventEmpty set in xArgs takes precedence unless that is set to OFF.
   */
  TriState preventEmptyArgs() default TriState.UNSET;

  /**
   * Similar to preventEmptyArgs but deals with cells having an Error like =NA() or = 1/0
   *
   * <p>The default is to call the function even if there are errors. Some special errors are
   * ignored, like: #NULL!, these cells became null and #NUM! this will became a NaN.
   *
   * <p>preventError set in xArgs takes precedence unless that is set to OFF.
   */
  TriState preventErrorArgs() default TriState.TRUE;

  /**
   * Specifies if excel plugin should invoke the specified functions if one of the arguments is not
   * provided to the function call. This not applicable to ranges of arrays or matrices because you
   * can not represent a missing value in a range.
   *
   * <p>eg: if func takes 3 parametes, then the following wont be called if preventMissingArgs =
   * true
   *
   * <p>=func(), =func(A1), =func(A1,B2), =func(A1,,C1)
   *
   * <p>The default is to call the function even if there are missing parameters.
   *
   * <p>preventMissing set in xArgs takes precedence unless that is set to OFF.
   */
  TriState preventMissingArgs() default TriState.UNSET;

  /**
   * This is a union of preventEmptyArgs, preventErrorArgs, and preventMissingArgs. Generally users
   * will set just this argument to prevent all error conditions.
   *
   * <p>The default is false.
   *
   * <p>preventInvalid set in xArgs takes precedence unless that is set to OFF.
   */
  TriState preventInvalidArgs() default TriState.UNSET;

  /**
   * Specifies the timeout value for synchronous executions in milliseconds. If not specified,
   * default is 60000 milliseconds.
   */
  int timeout() default 60000;

  /**
   * Specifies if this @xFunc is thread-safe from the point of view of Excel's multi-threaded
   * calculation. If IsAsync is set to true or return value is an object handle, which means RTD
   * server is used in the excel function, then this function can't be thread-safe as Excel
   * considers all RTD calls as non thread-safe.
   */
  boolean isThreadSafe() default false;

  /**
   * Specifies supported interop targets such as Excel or Python. For e.g., if target contains
   * *only* Excel, then this exported method can only invoked from Excel. By default it's empty,
   * meaning all targets can invoke that particular exported method.
   */
  InteropTarget[] supportedTargets() default {};
}
