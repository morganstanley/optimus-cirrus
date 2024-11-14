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
package optimus.examples.platform02.graph

import optimus.platform._

@entity class FieldExtractor[A](o: A) {

  // we need to use OptimusPartialFunction instead of scala original PartialFunction
  @node def extract[B](f: OptimusPartialFunction[A, B]): Option[B] =
    if (f.isDefinedAt(o)) {
      Option(f(o))
    } else {
      None
    }
}

@entity object NodeCaller {
  @node def func[T](t: T) = {
    println(s"inside NodeCaller.func ${t}")
    t
  }

  func_info.cacheable = false // disable the node cache
}

@entity class MyEntity(iValue: Int) extends FieldExtractor(iValue) {

  def syncContextCollect: Seq[Int] = {
    (1 to 10).apar collect {
      case i: Int if i > 5 => NodeCaller.func(i)
    }
  }

  @node def asyncCollect: Seq[Int] = {
    (1 to 10).apar collect {
      case i: Int if i > 5 => NodeCaller.func(i)
    }
  }
}

object AsyncCollect extends LegacyOptimusApp {
  val e = MyEntity(42)

  println("start async collect in async context: ")
  e.asyncCollect foreach println

  println("start async collect in sync context: ")
  e.syncContextCollect foreach println
}
