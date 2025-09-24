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
package optimus.platform.pickling;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is for a very specific and hopefully narrow use case to support creating
 * instances of case objects and enums from string representations where the string representations
 * require special handling. Such special handling is known to be required when data has already
 * been stored in the DAL with some old string representation and there is a desire to change the
 * string representations. They need to be applied to a function that will return the right instance
 * of the case object or enumeration value given the string. For case objects, the method needs to
 * be defined on the companion of the common sealed trait. For Enumerations, it needs to be defined
 * on the enumeration itself.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface unpickleConverter {}
