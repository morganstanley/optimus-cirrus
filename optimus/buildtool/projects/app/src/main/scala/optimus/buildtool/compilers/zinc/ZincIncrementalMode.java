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
package optimus.buildtool.compilers.zinc;

/*
 *  Mode for zinc incrementality. Options:
 *  - None:   Don't search for a previous zinc analysis file; always do a full scope compilation.
 *  - DryRun: Search for a previous zinc analysis file (and download remote compilation fingerprints
 *            if they exist), but don't use for the actual compilation. This is useful in order
 *            to view fingerprint diffs between this and earlier scope compilations, to see
 *            why we didn't get a perfect cache hit.
 * - Full:    Search for a previous zinc analysis file (and download all required remote artifacts),
 *            and perform an incremental build using the previous analysis and artifacts.
 */
public enum ZincIncrementalMode {
  None,
  DryRun,
  Full
}
