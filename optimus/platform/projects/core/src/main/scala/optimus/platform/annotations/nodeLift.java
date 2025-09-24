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

/*
 * Used by compiler plugin to produce the following transformation at call site:
 *
 *   1. for an annotated def:
 *      @nodeLift def method[T](arg1 [, arg2]) -> method$node[T](arg1$newNode [, arg2$newNode])
 *
 * 2. for an annotated parameter:
 *     a. def method(@nodeLift arg: => T) -> method( new AlreadyCompletedNode(arg) )
 *     b. def method(@nodeLift @nodeLiftByName arg: => T) -> method( new Node[T] { completeWithResult(arg) } ) (See @nodeLiftByName)
 *     c. def method(@nodeLift @nodeLiftByName @propertyNodeLift arg: => T) -> method( new PropertyNode[T] { completeWithResult(arg) } ) (See @propertyNodeLift)
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
public @interface nodeLift {}
