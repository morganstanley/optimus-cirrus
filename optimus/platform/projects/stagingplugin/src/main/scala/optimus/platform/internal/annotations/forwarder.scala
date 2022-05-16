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
package optimus.platform.internal.annotations

import scala.annotation.StaticAnnotation

/**
 * Induces a rewrite of a final val to its referee. Scala does this already for scalar values: writing
 * {{{
 *   final val x = 1
 *   def y = x
 * }}}
 * will cause the bytecode of "y" to be identical to that of `def y = 1`. This rewrite doesn't happen for non-scalar
 * values, which is usually the behavior you'd expect. However, the "forwarder" pattern this enables means that we have
 * package objects with a large number of vals in them. Accessing any one of these vals induces the evaluation of every
 * one of those vals and the necessary class loading. This has the downside of triggering `<clinit>` of all of those
 * classes, which does who-knows-what. (This includes running the module constructors for any referenced `object`s.)
 * This can cause (did cause) issues even outside of the macro context (running too early, random library `<clinit>`s)
 * and is also unnecessary. This is, admittedly, kind of a hack, but it's a hack which works well and can be temporary.
 */
private[optimus] final class forwarder extends StaticAnnotation
