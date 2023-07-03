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
  int slotNumber();

  boolean explicitSlotNumber();

  boolean isStorable();

  boolean isTrait();

  boolean isObject();

  boolean projected();

  boolean fullTextSearch();

  // these fields are in line with the attribute names
  // if we add/modify fields in this annotation then we need to keep this is step
  // additionally the code that generates this annotation in OptimusNames, AdjustAst should be kept
  // in line
  // this is also parsed in code, so MetaDataReader and EntityHierarchyManager will need
  // corresponding changes

  public static String name_slotNumber = "slotNumber";
  public static String name_explicitSlotNumber = "explicitSlotNumber";
  public static String name_isStorable = "isStorable";
  public static String name_isTrait = "isTrait";
  public static String name_isObject = "isObject";
  public static String name_projected = "projected";
  public static String name_fullTextSearch = "fullTextSearch";
}
