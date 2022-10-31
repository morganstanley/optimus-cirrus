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
package optimus.collection

import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet
import scala.collection.immutable.TreeMap
import scala.collection.immutable.TreeSet

object RedBlackSupport {

  // expose some 2.13 features for SortedMaps.
  // The common RedBlackTree was backported, but the APIs could not be made available directly
  // due to binary compat, so we loop through a java class o make the call, as java can still call the methods needed
  implicit class TreeMapOps[A, B](val map: SortedMap[A, B]) extends AnyVal {
    @inline private def treeMap = map match {
      case tm: TreeMap[A, B] => tm
      case _                 =>
        // should not happen - API should only be used on TreeMaps, but in case it isnt
        (TreeMap.newBuilder[A, B](map.ordering) ++= map).result()
    }
    /*
     * returns the key and value of the entry whose key is < the supplied key, or None
     */
    def maxBefore(key: A): Option[(A, B)] = {
      RedBlackHelper.maxBefore(key, treeMap)
    }
    /*
     * returns the key and value of the entry whose key is >= he supplied key, or None
     */
    def minAfter(key: A): Option[(A, B)] = {
      RedBlackHelper.minAfter(key, treeMap)
    }
  }
  // expose some 2.13 features for SortedSets.
  // The common RedBlackTree was backported, but the APIs could not be made available directly
  // due to binary compact, so we loop through a java class o make the call, as java can still call the methods needed
  implicit class TreeSetOps[A, B](val map: SortedSet[A]) extends AnyVal {
    @inline private def treeSet = map match {
      case ts: TreeSet[A] => ts
      case _              =>
        // should not happen - API should only be used on TreeMaps, but in case it isn't
        (TreeSet.newBuilder[A](map.ordering) ++= map).result()
    }
    /*
     * returns the key and value of the entry whose key is < the supplied key, or None
     */
    def maxBefore(key: A): Option[A] = {
      RedBlackHelper.maxBefore(key, treeSet)
    }
    /*
     * returns the key and value of the entry whose key is >= he supplied key, or None
     */
    def minAfter(key: A): Option[A] = {
      RedBlackHelper.minAfter(key, treeSet)
    }
  }
}
