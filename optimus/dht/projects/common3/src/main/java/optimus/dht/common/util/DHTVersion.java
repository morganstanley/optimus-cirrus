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
package optimus.dht.common.util;

/** A small utility that reads version from MANIFEST.MF file. */
public final class DHTVersion {

  private static final String version;

  private DHTVersion() {}

  static {
    // our build system adds 'Implementation-Version' attribute to jar's MANIFEST.MF file
    String packageVersion = DHTVersion.class.getPackage().getImplementationVersion();
    // version is not available if library is not packaged into jar
    version = packageVersion != null ? packageVersion : "<unknown>";
  }

  /**
   * Returns version of the library, if set in a jar file.
   *
   * @return version of the library.
   */
  public static String getVersion() {
    return version;
  }
}
