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
package com.ms.silverking.cloud.dht;

/**
 * Mode of revisions supported by a namespace. A revision is the creation of a value with a version
 * &lt;= the latest stored version (creation time is always monotonically increasing.)
 */
public enum RevisionMode {
  /** No revisions allowed */
  NO_REVISIONS,
  /** Unrestricted revisions allowed */
  UNRESTRICTED_REVISIONS
}
