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
package optimus.datatype.datacatalog
import optimus.datatype.Classification.DataSubjectCategory
import optimus.datatype.FieldPiiClassification

import scala.annotation.meta.field
import scala.annotation.meta.getter

@field
@getter
// docs-snippet:FieldMetaAnnotation
final case class fieldMeta(
    /**
     * Free text for adding field description: Optional
     */
    description: String,
    /**
     * PII classification for Field: Optional
     */
    piiClassification: List[FieldPiiClassification[_ <: DataSubjectCategory]]
) extends annotation.StaticAnnotation
// docs-snippet:FieldMetaAnnotation
    {
  def this(description: String) = this(description, null)
  def this(piiClassification: List[FieldPiiClassification[_ <: DataSubjectCategory]]) = this(null, piiClassification)
}
