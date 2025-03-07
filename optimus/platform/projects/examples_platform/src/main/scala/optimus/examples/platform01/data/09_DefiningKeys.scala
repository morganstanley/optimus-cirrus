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
package optimus.examples.platform01.data

import optimus.platform._

import java.time.ZonedDateTime
import optimus.examples.platform.entities.SimpleEvent

// This entity shows a secondary key
@stored @entity
class Animal(@key val commonName: String, @indexed(unique = true) val scientificName: String) {
  def this() = this("", "")
}

// this entity shows a key that is composed of more than 1 value
@stored @entity
class Person(val firstName: String, val secondName: String, @node val dob: ZonedDateTime = ZonedDateTime.now) {
  @entersGraph def dob$init(): ZonedDateTime = ZonedDateTime.now

  @key def theKey = (firstName, secondName)
}

object DefiningKeys extends OptimusApp {
  val user = System.getProperty("user.name")

  override def setup(): Unit = {
    if (cmdLine.uri == null)
      cmdLine.uri = "broker://dev?context=named&context.name=" + user
  }

  @entersGraph override def run(): Unit = {
    def writeAnimal(commonName: String, scientificName: String) = {
      newEvent(SimpleEvent.uniqueInstance(s"CreateAnimal $commonName")) {
        DAL.put(Animal(commonName, scientificName))
      }
    }

    def writePerson(firstName: String, lastName: String) = {
      newEvent(SimpleEvent.uniqueInstance(s"CreatePerson $firstName $lastName")) {
        DAL.put(Person(firstName, lastName))
      }
    }

    def populate(): Unit = {
      writeAnimal("Giraffe", "Giraffa camelopardalis")
      writeAnimal("Dog", "Canis familiaris domesticus")
      writeAnimal("Hippopotamus", "Hippopotamus amphibius")
      writeAnimal("Donkey", "Equus africanus asinus")
      writePerson("Fred", "Bloggs")
      writePerson("Joe", "Bloggs")
    }
    // make sure the entities are in the store
    DAL.purgePrivateContext()
    populate()

    given(validTimeAndTransactionTimeNow) {
      // retrieve an entity by common name
      val dog = Animal.get("Dog")

      // retrieve an entity by scientific name
      val sameDog = Animal.scientificName.get("Canis familiaris domesticus")

      require(dog.commonName == sameDog.commonName)
      require(dog.scientificName == sameDog.scientificName)

      println("Scientific name for " + dog.commonName + " is " + dog.scientificName)

      // get another animal by scientific name
      val name = "Equus africanus asinus"
      println("Common name for " + name + " is " + Animal.scientificName.get(name).commonName)

      // the other way around
      val name2 = "Giraffe"
      println("Scientific name for " + name2 + " is " + Animal.get(name2).scientificName)

      // get method matches the signature of the key
      val person = Person.get("Joe", "Bloggs")
      println("There is a person called Joe: " + person)
    }

    DAL.purgePrivateContext()
  }
}
