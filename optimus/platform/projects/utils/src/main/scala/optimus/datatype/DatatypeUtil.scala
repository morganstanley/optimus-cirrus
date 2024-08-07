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

package optimus.datatype

import optimus.datatype.Classification.DataSubjectCategory
import optimus.platform.metadatas.internal.PIIDetails

import java.lang.reflect.Field

object DatatypeUtil {

  def extractPiiElementsList(declaredFields: Array[Field]): Seq[PIIDetails] = {
    declaredFields.collect {
      case field
          if field.getType.getInterfaces.toList
            .map(_.getName)
            .contains(classOf[PIIElement[_ <: DataSubjectCategory]].getName) =>
        PIIDetails(
          field.getName,
          field.getType.getCanonicalName,
          field.getType.getTypeParameters.map(_.getName.split("$").last).toSeq
        )
    }.toSeq
  }

}
