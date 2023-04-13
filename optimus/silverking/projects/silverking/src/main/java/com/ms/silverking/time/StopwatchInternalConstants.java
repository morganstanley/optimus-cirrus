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
package com.ms.silverking.time;

import java.math.BigDecimal;

class StopwatchInternalConstants {
  static final double nanosPerSecond = 1000000000.0;
  static final BigDecimal nanosPerSecondBD = new BigDecimal(nanosPerSecond);
  static final long millisPerSecond = 1000;
  static final long nanosPerMilli = 1000000;
}
