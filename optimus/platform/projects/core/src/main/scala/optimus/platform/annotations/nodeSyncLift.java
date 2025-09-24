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
package optimus.platform.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks method foo( arg* ): T to have a companion method foo$withNode( arg* ): T
 *
 * <p>By name args marked with nodeLift will be converted to Node[T]
 *
 * <p>nodeSyncLift loses to nodeSync in async context
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
public @interface nodeSyncLift {}
