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
package optimus.platform.temporalSurface

import optimus.exceptions.RTExceptionTrait

sealed abstract class TemporalSurfaceException(msg: String, cause: Throwable)
    extends Exception(msg, cause)
    with RTExceptionTrait

/**
 * the query being executed does not match the temporal surface e.g. the temporal surface has no understanding of the
 * mapping to a temporality of the appropriate type
 */
final class TemporalSurfaceNoDataException(msg: String) extends TemporalSurfaceException(msg, null)

/**
 * cannot determine temporality
 */
final class TemporalSurfaceTemporalityException(msg: String) extends TemporalSurfaceException(msg, null)
