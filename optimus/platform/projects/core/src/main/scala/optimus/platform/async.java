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

/** It tells entity-plugin only do async transform (especially in non-entity types). */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.METHOD})
public @interface async {

  /**
   * Former @createNodeTrait. It tells the plugin to create a "trait foo$node" on the entity class
   * with fields for all of the node parameters. The actual (hidden) node class will implement this
   * trait. This is mainly useful for plugins which need to extract fields from the node.
   */
  boolean exposeArgTypes() default false;
}
