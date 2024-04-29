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
package optimus.platform.annotations.internal;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EntityMetaDataAnnotation {

  int slotNumber() default 0;

  boolean explicitSlotNumber() default false;

  boolean isStorable() default false;

  boolean isTrait() default false;

  boolean isObject() default false;

  boolean projected() default false;

  boolean fullTextSearch() default false;

  // these fields are in line with the attribute names
  // if we add/modify fields in this annotation then we need to keep this is step
  // additionally the code that generates this annotation in OptimusNames,
  // AdjustAst should be kept in line this is also parsed in code,
  // so MetaDataReader and EntityHierarchyManager will need corresponding changes

  String name_slotNumber = "slotNumber";
  String name_explicitSlotNumber = "explicitSlotNumber";
  String name_isStorable = "isStorable";
  String name_isTrait = "isTrait";
  String name_isObject = "isObject";
  String name_projected = "projected";
  String name_fullTextSearch = "fullTextSearch";
}
