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
package optimus.dal.storable

import optimus.platform.storable.AppEventReference
import optimus.platform.dsi.bitemporal.Obliterate
import java.time.Instant

/**
 * This is the container class for the ObliterateAction, and will be mapped to a mongo doc that will be stored as
 * obliterate effect.
 *
 * XXX: the app event reference will be used as identifier. we may need to change it to have its own id if we want to
 * allow multiple obliterates in one single app event.
 */
final case class ObliterateEffect(id: AnyRef, appEventId: AppEventReference, obliterate: Obliterate)
