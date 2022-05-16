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
package optimus.collection;

import java.lang.reflect.Field;

import scala.Option;
import scala.collection.immutable.RedBlackTree;
import scala.collection.immutable.RedBlackTree$;
import scala.collection.immutable.TreeMap;
import scala.collection.immutable.TreeSet;
import scala.math.Ordering;

class RedBlackHelper {
  private RedBlackHelper() {}

  private final static Field treeMapField;
  private final static Field treeSetField;

  static {
    try {
      treeMapField = TreeMap.class.getDeclaredField("tree");
      treeMapField.setAccessible(true);
      treeSetField = TreeSet.class.getDeclaredField("tree");
      treeSetField.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  static <A, B> scala.Option<scala.Tuple2<A, B>> maxBefore(A key, TreeMap<A, B> map) throws IllegalAccessException {
    RedBlackTree.Tree<A, B> tree = (RedBlackTree.Tree<A, B>) treeMapField.get(map);
    Ordering<A> ordering = map.ordering();
    RedBlackTree.Tree<A, B> result = RedBlackTree$.MODULE$.maxBefore(tree, key, ordering);
    scala.Tuple2<A, B> y = result == null ? null : new scala.Tuple2<A, B>(result.key(), result.value());
    return Option.apply(y);
  }

  static <A, B> scala.Option<scala.Tuple2<A, B>> minAfter(A key, TreeMap<A, B> map) throws IllegalAccessException {
    RedBlackTree.Tree<A, B> tree = (RedBlackTree.Tree<A, B>) treeMapField.get(map);
    Ordering<A> ordering = map.ordering();
    RedBlackTree.Tree<A, B> result = RedBlackTree$.MODULE$.minAfter(tree, key, ordering);
    scala.Tuple2<A, B> y = result == null ? null : new scala.Tuple2<A, B>(result.key(), result.value());
    return Option.apply(y);
  }

  static <A> scala.Option<A> maxBefore(A key, TreeSet<A> set) throws IllegalAccessException {
    RedBlackTree.Tree<A, ?> tree = (RedBlackTree.Tree<A, ?>) treeSetField.get(set);
    Ordering<A> ordering = set.ordering();
    RedBlackTree.Tree<A, ?> result = RedBlackTree$.MODULE$.maxBefore(tree, key, ordering);
    A y = result == null ? null : result.key();
    return Option.apply(y);
  }

  static <A> scala.Option<A> minAfter(A key, TreeSet<A> set) throws IllegalAccessException {
    RedBlackTree.Tree<A, ?> tree = (RedBlackTree.Tree<A, ?>) treeSetField.get(set);
    Ordering<A> ordering = set.ordering();
    RedBlackTree.Tree<A, ?> result = RedBlackTree$.MODULE$.minAfter(tree, key, ordering);
    A y = result == null ? null : result.key();
    return Option.apply(y);
  }
}
