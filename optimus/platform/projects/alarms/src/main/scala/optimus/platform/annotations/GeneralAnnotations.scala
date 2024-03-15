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
 * It's similar to the Scala deprecated annotation which is used to mark a deprecated definition.
 * @param suggestion
 *   Informative message suggesting an alternative.
 */
@getter @setter @beanGetter @beanSetter
class deprecating(suggestion: String) extends StaticAnnotation

/**
 * same as @deprecating but will only be fatal in new/modified files (used to avoid merge race condition when merging
 * PRs which mark commonly used APIs as deprecating)
 */
class deprecatingNew(suggestion: String) extends StaticAnnotation

class discouraged(discouragedThing: String, reason: String) extends StaticAnnotation
