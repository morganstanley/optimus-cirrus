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
package optimus.scalacompat.collection;

public abstract class CanEqual {
  public static boolean canEqual(scala.collection.IterableOnce<?> p1, scala.collection.IterableOnce<?> p2) {
    if (p1 == p2)
      return true;
    if (p1 == null || p2 == null)
      return false;
    int size1 = p1.knownSize();
    int size2 = p2.knownSize();
    return size1 == -1 || size2 == -1 || size1 == size2;
  }

  public static boolean canEqual(String p1, String p2) {
    if (p1 == p2)
      return true;
    if (p1 == null || p2 == null)
      return false;
    return p1.length() == p2.length() && p1.hashCode() == p2.hashCode();
  }
  //TODO Stable, OptimusCollections, things that we patch a hashcode into etc
  //  public static boolean canEqual(Stable p1, Stable p2) {
  //    if (p1 == p2)
  //      return true;
  //    if (p1 == null || p2 == null)
  //      return false;
  //    return p1.getClass() == p2.getClass() && p1.hashCode() == p2.hashCode();
  //  }
  public static boolean canEqual(Enum<?> p1, Enum<?> p2) {
    return p1 == p2;
  }
  public static boolean canEqual(Object p1, Object p2) {
    return ((p1 == null) == (p2 == null));
  }
}
