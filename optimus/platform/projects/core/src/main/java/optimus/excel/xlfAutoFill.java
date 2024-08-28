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
package optimus.excel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Annotation to specify an xlf entity node as an auto fill function */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface xlfAutoFill {
  /**
   * Specifies if auto-fill behaviour is enabled. If no arguments are passed to the xlfAutoFill()
   * constructor, default is true. So to enable auto-fill, you can just say autoFill = new
   * xlfAutoFill().
   */
  boolean enabled() default true;

  AutoFillStart start() default AutoFillStart.NEXT_DOWN;
}
