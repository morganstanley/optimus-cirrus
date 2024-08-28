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
package optimus.platform.relational.tree;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Use this annotation to mark final defs on traits meant to be used as coordinates.
 *
 * <p>This allows the ScalaToolboxProxyFactory machinery to not attempt to override such methods.
 * This is needed because our custom TypeInfo version of TypeTags (needed because the latter are not
 * thread-safe) does not allow us to inspect the scala runtime fully, so final defs on traits appear
 * as public abstract java methods.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ProxyFinalModifier {}
