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
package optimus.graph

import optimus.platform.storable.Entity
import optimus.platform.temporalSurface.TemporalSurface

/**
 * Helper functions for scalac entity plugin. The general purpose of everything here and in PluginSupport (in graph) is
 * to aid code generation before the compiler's typer phase. Since explicit type information is not available, we
 * exploit type inference by passing variations on user-written code that has the appropriate type, and rely on the
 * inferencer's type propagation to correctly type our generated code.
 */
object CorePluginSupport {

  /** Plugin generates calls to this method. */
  final def observedValueNode[T](v: T, entity: Entity, propertyInfo: NodeTaskInfo): PropertyNode[T] = {
    val tc = entity.dal$temporalContext
    if (tc != null && tc.isInstanceOf[TemporalSurface] && tc.asInstanceOf[TemporalSurface].canTick)
      new InitedPropertyNodeSync(
        v,
        entity,
        propertyInfo.asInstanceOf[DefPropertyInfo0[Entity, Entity => Boolean, Entity => T, T]])
    else
      new AlreadyCompletedPropertyNode(v, entity, propertyInfo)
  }
}
