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

import optimus.platform.util.Log
import org.codehaus.jettison.json.JSONArray
import org.codehaus.jettison.json.JSONObject

object JsonUtils extends Log {
  def flatMapArrayOfObjects[R](jsonArray: JSONArray, f: JSONObject => Seq[R]): Seq[R] = {
    (0 until jsonArray.length).flatMap { i =>
      f(jsonArray.getJSONObject(i))
    }
  }
  def filterThenMapArrayOfObjects[R](jsonArray: JSONArray, cond: JSONObject => Boolean, f: JSONObject => R): Seq[R] = {
    (0 until jsonArray.length).foldLeft(Seq.empty: Seq[R])((seq, index) => {
      val jsonObj = jsonArray.getJSONObject(index)
      if (cond(jsonObj)) seq :+ f(jsonObj) else seq
    })
  }
  def mapArrayOfObjects[R](jsonArray: JSONArray, f: JSONObject => R): Seq[R] = {
    (0 until jsonArray.length).map { i =>
      f(jsonArray.getJSONObject(i))
    }
  }
  def arrayToObjects(jsonArray: JSONArray): Seq[JSONObject] = (0 until jsonArray.length()).flatMap { i =>
    val maybeObject = Option(jsonArray.optJSONObject(i))
    if (maybeObject.isEmpty) {
      log.warn(s"Unexpected element - expected object but got: ${jsonArray.opt(i)}")
    }
    maybeObject
  }

  def arrayToStrings(jsonArray: JSONArray): Seq[String] = (0 until jsonArray.length()).flatMap { i =>
    val maybeString = Option(jsonArray.optString(i))
    if (maybeString.isEmpty) {
      log.warn(s"Unexpected element - expected string but got: ${jsonArray.opt(i)}")
    }
    maybeString
  }

  def maybeString(fields: JSONObject, key: String): Option[String] = {
    if (isKeyMissing(fields, key) || Option(fields.optString(key)).isEmpty) None
    else Some(fields.getString(key))
  }

  def maybeObject(fields: JSONObject, key: String): Option[JSONObject] = {
    if (isKeyMissing(fields, key) || Option(fields.optJSONObject(key)).isEmpty) None
    else Some(fields.getJSONObject(key))
  }

  def maybeArray(fields: JSONObject, key: String): Option[JSONArray] = {
    if (isKeyMissing(fields, key) || Option(fields.optJSONArray(key)).isEmpty) None
    else Some(fields.getJSONArray(key))
  }

  def isKeyMissing(fields: JSONObject, key: String): Boolean =
    fields.isNull(key) || !fields.has(key) || Option(fields.opt(key)).isEmpty
}
