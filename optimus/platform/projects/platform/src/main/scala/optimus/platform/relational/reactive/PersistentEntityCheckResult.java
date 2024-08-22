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
package optimus.platform.relational.reactive;

public enum PersistentEntityCheckResult {
  /** All conditions are matched */
  MATCHED,
  /** Some of the conditions are not matched */
  UNMATCHED,
  /** Cannot check some of the conditions because the property doesn't exist */
  MISSING_PROPERTY;

  public PersistentEntityCheckResult and(PersistentEntityCheckResult other) {
    if (this == UNMATCHED || other == UNMATCHED) return UNMATCHED;
    else if (this == MATCHED) return other;
    else if (other == MATCHED) return this;
    else return MISSING_PROPERTY;
  }

  public PersistentEntityCheckResult or(PersistentEntityCheckResult other) {
    if (this == MATCHED || other == MATCHED) return MATCHED;
    else if (this == UNMATCHED) return other;
    else if (other == UNMATCHED) return this;
    else return MISSING_PROPERTY;
  }

  public static PersistentEntityCheckResult fromBoolean(boolean value) {
    return value ? MATCHED : UNMATCHED;
  }
}
