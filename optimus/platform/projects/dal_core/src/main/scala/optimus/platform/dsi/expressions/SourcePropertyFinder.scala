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
package optimus.platform.dsi.expressions

class SourcePropertyFinder(via: Property) extends ExpressionVisitor {
  private var source: Option[QuerySource] = None
  private var property: Property = via
  private var alias: Id = via.owner
  private var name: String = via.names.head

  protected def foundFromSelect = property ne via

  def find(e: Expression): Option[(QuerySource, Property)] = {
    visit(e)
    source.map(s => (s, property))
  }

  protected override def visitEntity(e: Entity): Expression = {
    if (e.id == alias) source = Some(e)
    e
  }

  protected override def visitLinkage(e: Linkage): Expression = {
    if (e.id == alias) source = Some(e)
    e
  }

  protected override def visitEmbeddable(e: Embeddable): Expression = {
    if (e.id == alias) source = Some(e)
    e
  }

  protected override def visitEvent(e: Event): Expression = {
    if (e.id == alias) source = Some(e)
    e
  }

  protected override def visitSelect(s: Select): Expression = {
    if (s.id == alias) {
      s.properties.find(p => p.name == name) match {
        case Some(PropertyDef(_, p @ Property(_, Seq(n), oid))) =>
          property = p
          alias = oid
          name = n
          visit(s.from)
        case _ =>
      }
      s
    } else super.visitSelect(s)
  }
}

object SourcePropertyFinder {
  def findEntityProperty(e: Expression, via: Property): Option[(Entity, Property)] = {
    find(e, via) match {
      case s @ Some((e: Entity, p)) => Some(e, p)
      case _                        => None
    }
  }

  def findLinkageProperty(e: Expression, via: Property): Option[(Linkage, Property)] = {
    find(e, via) match {
      case s @ Some((e: Linkage, p)) => Some(e, p)
      case _                         => None
    }
  }

  def findEmbeddableProperty(e: Expression, via: Property): Option[(Embeddable, Property)] = {
    find(e, via) match {
      case s @ Some((e: Embeddable, p)) => Some(e, p)
      case _                            => None
    }
  }

  def find(e: Expression, via: Property): Option[(QuerySource, Property)] = {
    val spf = new SourcePropertyFinder(via)
    spf.find(e)
  }
}
