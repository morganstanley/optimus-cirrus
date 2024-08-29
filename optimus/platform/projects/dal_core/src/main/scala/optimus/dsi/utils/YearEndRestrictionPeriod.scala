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
package optimus.dsi.utils

import java.time.Instant

import optimus.platform.TimeInterval

object YearEndRestrictionPeriod {

  val PropertyName = "optimus.dal.yearend.restriction.period"

  def get(): Option[TimeInterval] = {
    Option(System.getProperty(PropertyName)).map { parse(_) }
  }

  def parse(start: String, end: String): TimeInterval = {
    TimeInterval(Instant.parse(start), Instant.parse(end))
  }

  /*
   * e.g. 2020-12-12T09:56:28.496Z-to-2021-01-12T09:56:28.496Z
   */
  def parse(value: String): TimeInterval = {
    val values = value.split("\\-to\\-")
    val start = values(0)
    val end = values(1)
    parse(start, end)
  }
}
