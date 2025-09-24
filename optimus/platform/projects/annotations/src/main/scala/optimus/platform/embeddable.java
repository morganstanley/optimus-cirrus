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
 * @see <a
 *     href="http://optimusguide/CoreAnnotations/BasicAnnotations.html">http://optimusguide/CoreAnnotations/BasicAnnotations.html</a>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface embeddable {
  /**
   * This should only be used when absolutely necessary, primarily for supporting backward
   * compatibility for data already saved in the DAL. The class specified must extend the
   * CustomPickling trait and provide implementations for pickle and unpickle methods.
   *
   * <p>The optimus runtime will automatically detect and use the custom implementation when
   * pickling/unpickling instances of the type.
   *
   * @return The class that implements custom pickling for this embeddable type
   */
  // This argument is matched by-name in VersionUtilsBase.scala so any change to its
  // name needs to change that string reference as well
  Class<?> customPickling() default Void.class;

  /**
   * Indicates whether members of this embeddable should be projected into relational form. When set
   * to true, members of the embeddable can be marked with @projected to enable faster PRIQL queries
   * against those fields.
   *
   * @return true if this embeddable should be projected, false otherwise
   */
  boolean projected() default false;
}
