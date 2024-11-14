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
package optimus.examples.platform.entities

import optimus.platform.{entity, node, key}
import optimus.platform.stored
import java.time.LocalDate

@stored @entity
class HolidayCalendar(
    val name: String,
    @node val holidays: List[(LocalDate, String)],
    @node val currency: String,
    @node val country: String,
    @node val settlementType: String,
    @node val description: String,
    @node val note1: String,
    @node val note2: String) {}
