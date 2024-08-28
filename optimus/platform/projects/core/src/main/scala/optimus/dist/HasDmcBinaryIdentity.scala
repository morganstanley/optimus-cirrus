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
package optimus.dist

/**
 * Classes extending this trait have control over their DMC binary identity. DMC uses binary identity when calculating
 * cache keys.
 *
 * The class name won't be appended to the calculated binary stream. If objects of two different classes return the same
 * result from [[HasDmcBinaryIdentity#dmcBinaryIdentity]] method, those objects will be considered identical for cache
 * key generation purposes. Use [[HasDmcBinaryIdentityWithType]] if you require different semantic.
 *
 * If a class extends both [[HasDmcBinaryIdentity]] and [[HasDmcBinaryIdentityWithType]], only [[HasDmcBinaryIdentity]]
 * will be used.
 */
trait HasDmcBinaryIdentity {

  /**
   * Method called to obtain DMC binary identity. Object returned from this method will be serialized, and its binary
   * representation will be used as a part of cache key.
   *
   * It's highly recommended that returned objects have meaningful implementation of hashCode/equals.
   *
   * @return
   *   object used as a binary identity source
   */
  def dmcBinaryIdentity: java.io.Serializable
}

/**
 * Classes extending this trait have control over their DMC binary identity. DMC uses binary identity when calculating
 * cache keys.
 *
 * The class name will be appended to the generated binary stream. If objects of two different classes return the same
 * result from [[HasDmcBinaryIdentityWithType#dmcBinaryIdentityWithType]] method, those objects will be considered
 * different for cache key generation purposes. Use [[HasDmcBinaryIdentity]] if you require different semantic.
 *
 * If a class extends both [[HasDmcBinaryIdentity]] and [[HasDmcBinaryIdentityWithType]], only [[HasDmcBinaryIdentity]]
 * will be used.
 */
trait HasDmcBinaryIdentityWithType {

  /**
   * Method called to obtain DMC binary identity. DMC will append full type name to the object returned from this method
   * before serializing it. Binary serialization stream will be then used as a part of cache key.
   *
   * It's highly recommended that returned objects have meaningful implementation of hashCode/equals.
   *
   * @return
   *   object used as a binary identity source
   */
  def dmcBinaryIdentityWithType: java.io.Serializable
}
