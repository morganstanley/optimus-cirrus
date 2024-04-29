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
 * Mark the annotated class an entity. Or the annotated trait for use as an entity interface or
 * mixin.
 *
 * <p>This annotation is processed at compile time by the Optimus 'entity' scalac plugin. Used
 * lower-case name to avoid collision between annotation and regular class names (esp for Entity).
 *
 * <p>An @stored @entity class is rewritten to extend an Entity base, and Entity member annotations
 * will be processed.
 *
 * @see http://optimusdoc/BasicAnnotations
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.CONSTRUCTOR)
public @interface entity {

  int schemaVersion() default 0;
}
