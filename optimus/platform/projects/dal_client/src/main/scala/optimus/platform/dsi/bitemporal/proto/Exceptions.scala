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
package optimus.platform.dsi.bitemporal.proto

import optimus.platform.dsi.bitemporal.TemporalContextException
import optimus.platform.dsi.bitemporal.proto.Dsi.TemporalContextProto
import optimus.platform.temporalSurface.TemporalSurface

class InvalidTemporalSurfaceMatcherException(m: Any)
    extends TemporalContextException(s"Only all, none, forClass, and forPackage matchers supported, not $m")

class InvalidTemporalSurfaceScopeMatcherException(ts: TemporalSurface)
    extends TemporalContextException(
      s"Cannot serialize a TemporalSurface with a defined scope matcher like ${ts.oneLineSummary}")

class InvalidTemporalSurfaceScopeMatcherProtoException(proto: TemporalContextProto)
    extends TemporalContextException(
      s"Cannot deserialize a TemporalSurfaceProto with a defined scope matcher like $proto")
