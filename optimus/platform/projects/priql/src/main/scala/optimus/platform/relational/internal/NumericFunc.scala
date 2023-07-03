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
package optimus.platform.relational.internal

object FuncDivide {

  def evaluate(l: Double, r: Double): Double = {

    l / r
  }
}

object FuncPlus {

  def evaluate(l: Double, r: Double): Double = {

    l + r
  }

  def evaluate(l: Float, r: Float): Float = {

    l + r
  }

  def evaluate(l: Long, r: Long): Long = {

    l + r
  }

  def evaluate(l: Int, r: Int): Int = {

    l + r
  }

  def evaluate(l: Short, r: Short): Int = {

    l + r
  }

  def evaluate(l: Byte, r: Byte): Int = {

    l + r
  }
}

object FuncMinus {

  def evaluate(l: Double, r: Double): Double = {

    l - r
  }

  def evaluate(l: Float, r: Float): Float = {
    l - r
  }

  def evaluate(l: Long, r: Long): Long = {

    l - r
  }

  def evaluate(l: Int, r: Int): Int = {

    l - r
  }

  def evaluate(l: Short, r: Short): Int = {

    l - r
  }

  def evaluate(l: Byte, r: Byte): Int = {

    l - r
  }

}

object FuncMultiply {

  def evaluate(l: Double, r: Double): Double = {

    l * r
  }

  def evaluate(l: Float, r: Float): Float = {

    l * r
  }

  def evaluate(l: Long, r: Long): Long = {

    l * r
  }

  def evaluate(l: Int, r: Int): Int = {

    l * r
  }

  def evaluate(l: Short, r: Short): Int = {

    l * r
  }

  def evaluate(l: Byte, r: Byte): Int = {

    l + r
  }

}

object FuncModulo {

  def evaluate(l: Double, r: Double): Double = {

    l % r
  }

  def evaluate(l: Float, r: Float): Float = {

    l % r
  }

  def evaluate(l: Long, r: Long): Long = {

    l % r
  }

  def evaluate(l: Int, r: Int): Int = {

    l % r
  }

  def evaluate(l: java.lang.Short, r: java.lang.Short): Int = {

    l % r
  }

  def evaluate(l: Byte, r: Byte): Int = {

    l % r
  }

}
