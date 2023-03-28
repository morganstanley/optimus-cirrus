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

import scala.collection.IterableOps;
import scala.collection.mutable.Builder;

/**
 * Scala's IterableOps.newSpecificBuilder is protected, which causes problems in AsyncBase, where we
 * need to create new instances of wrapped collection classes. Unfortunately, Iterable.companion
 * returns companion objects that are degenerate, e.g. we get the same builder for every variety of
 * Set.
 *
 * <p>This workaround exposes newSpecificBuilder via Java, which does not support Scala protected
 */
public abstract class BuilderProvider {
  public static Builder exposedBuilder(Object coll) {
    return ((IterableOps) coll).newSpecificBuilder();
  }

  public static String className(Object coll) {
    return ((scala.collection.Iterable) coll).className();
  }
}
