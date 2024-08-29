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
package optimus.platform.internal;

enum MarkerResolvers implements optimus.platform.internal.MarkerResolver<MarkerResolvers> {
  validTimeMarker,
  transactionTimeMarker,
  loadContextMarker,
  validTimeStoreMarker;

  optimus.platform.internal.MarkerResolver<?> asMarkerResolver() {
    return this;
  }

  static {
    // force initialisation of the markers
    TemporalSource$.MODULE$.loadContextMarker();
  }
}
