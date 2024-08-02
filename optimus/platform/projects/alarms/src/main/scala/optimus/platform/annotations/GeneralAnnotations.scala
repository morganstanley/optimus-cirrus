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
package optimus.platform.annotations

import scala.annotation.StaticAnnotation
import scala.annotation.meta.beanGetter
import scala.annotation.meta.beanSetter
import scala.annotation.meta.getter
import scala.annotation.meta.setter

/**
 * This annotation is used to mark a deprecated definition. It should be used in Optimus code instead of Scala's
 * built-in `@deprecated`. See http://codetree-docs/optimus/docs/CodeStandards/WarningsAndDeprecations.md
 * @param suggestion
 *   Informative message suggesting an alternative.
 */
@getter @setter @beanGetter @beanSetter
class deprecating(suggestion: String) extends StaticAnnotation

/**
 * This annotation is the same as [[deprecating]] but will only be fatal in new/modified files.
 * This is helpful for deprecating a widely used definition, to avoid merge race conditions or avoid
 * having to update all uses in codetree at once.
 * See http://codetree-docs/optimus/docs/CodeStandards/WarningsAndDeprecations.md
 */
@getter @setter @beanGetter @beanSetter
class deprecatingNew(suggestion: String) extends StaticAnnotation

/**
 * This is an internal annotation, don't use it in source code. Use [[deprecating]] / [[deprecatingNew]] instead.
 *
 * Optimus adds the `@discouraged` annotation to certain library symbols, such as `view` on Scala collections.
 * Usages of these symbols trigger a warning (id 10006, DISCOURAGED_CONSTRUCT).
 */
class discouraged(discouragedThing: String, reason: String) extends StaticAnnotation
