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

import optimus.excel.input.DefaultInvalidHandleErrorGenerator;

/** Annotation to provide default primitive return values for excel functions */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface defaultsProvider {
  boolean enabled() default true;

  int intVal() default 0;

  double doubleVal() default 0.0;

  boolean boolVal() default false;

  String stringVal() default "";

  boolean generateError() default false;

  Class<?> invalidHandleErrorGenerator() default DefaultInvalidHandleErrorGenerator.class;
}
