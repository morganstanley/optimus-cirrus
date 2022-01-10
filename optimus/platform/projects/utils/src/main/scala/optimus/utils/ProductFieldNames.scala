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
package optimus.utils

trait ProductFieldNames { this: Product =>

  /**
   * Produce an iterator which returns the names of the fields of this case class, in the order that [[productIterator]]
   * would return them. This functionality will become part of core Scala in 2.13.
   */
  final def productFieldNames: Iterator[String] = ProductFieldNames.productFieldNamesCache.get(getClass).iterator
}

object ProductFieldNames {
  private object productFieldNamesCache extends ClassValue[Array[String]] {
    override def computeValue(tpe: Class[_]): Array[String] = {
      if (tpe.getConstructors.isEmpty) Array.empty
      else {
        val ctor = tpe.getConstructors.maxBy(_.getParameterCount)
        ctor.getParameters.map(_.getName)
      }
    }
  }
}
