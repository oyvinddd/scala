
object Main extends App {


    // ###################
    // ##  EXPRESSIONS  ##
    // ###################

    // NOTE: var = variable, val = constant/immutable

    // expression with result type
    val res: Int = 5 + 10
    // string expression with type inference
    val str = "Hello" + "World"

    println(res, str)


    // ####################
    // ##  CONDITIONALS  ##
    // ####################

    // conditionals return values:
    val cond = if(res > 10) {
        res + " is greater than 10"
    } else {
        res + " is NOT greater than 10"
    }
    print(cond)

    // NOTE! Scala doesn't need an explicit return statement


    // #############
    // ##  LOOPS  ##
    // #############

    // simple for-loop
    for(letter <- List(1,4,3,8,9,3,5)) {
        println(letter)
    }

    // for-loop with condition(s)
    for(letter <- List(1,4,3,8,9,3,5) if letter % 2 == 0 if letter != 4) {
        println("Letter: " + letter)
    }

    // nested for-loops can be written like this (or the traditional way)
    val numbers = List(1,2,3)
    val letters = List("a", "b", "c")
    for {
        number <- numbers
        letter <- letters
    } println(number + " - " + letter)

    // NOTE! Curly braces are optional if there is only one expression in the body

    // functional for-loop (notice they return values)
    val exp: List[Int] = for(number <- List(1,2,3)) yield number * 2
    println(exp)


    // #################
    // ##  FUNCTIONS  ##
    // #################

    //  simple function (return type is optional)
    def plusOneOrZero(number: Int) = {
        if(number < 0) 0
        else number + 1
    }

    // single-line function
    def product(n1: Int, n2: Int) = n1 * n2

    // Remember, this is the function syntax: (argument1: Type, argument2: Type) => Body

    // function literal
    val plusOne = (x: Int) => x + 1

    case class Fruit(name: String)

    val apple = Fruit("apple")
    val pear = Fruit("pear")
    var banana = Fruit("banana")

    // filter example
    val basket: List[Fruit] = List(apple, pear, banana)
    var filtered = basket.filter((fruit: Fruit) => fruit.name != "apple")
    // ...or shorthand
    filtered = basket.filter(_.name != "apple")
    println(filtered)


    // ###############
    // ##  CLASSES  ##
    // ###############

    class Employee {
        private var salary = 100

        def getSalary() = salary
    }

    val robert = new Employee
    val mary = new Employee

    println(robert, mary)

    // companion objects:
    //      used when we don't want to instantiate a class to use a function etc.
    //      they share the same name as the class, they start with object keyword,
    //      live in the same source file as the class, can access private members

    // this is the class
    class MathCompanion {
        private val max = 100
    }

    // ...and this is the companion object
    object MathCompanion {
        def sum(a: Int, b: Int) = a + b
    }

    // apply keyword:
    //      - built-in utility for instantiating a new object
    class Person(firstName: String, lastName: String) {
        def getName: String = firstName + " " + lastName
    }
    
    object Person {
        def apply(firstName: String, lastName: String): Person = new Person(firstName, lastName)
    }

    // case classes (used for convenience):
    //      - companion objects and apply methods are auto-generated in case of using this syntax
    //      - all arguments are immutable
    //      - adds copy method for modified copies
    //      - pattern matching, toString, hash etc.

    case class Course(title: String, author: String)

    val scalaCourse = ("Scala: The Big Picture", "Harit Himanshu")
    println(scalaCourse.toString())

    // traits:
    //      - App trait: contains main/program entry point
    //      - see top of this file (main method is called implicitly)


    // ######################
    // ##  ERROR HANDLING  ##
    // ######################

    // some & none

    val people: List[String] = List("Robert", "Mary")
    // val result: Option[String] = people.find(_ == "Mary").get

    val maybeMary = people.find(_ == "Mary")
    if(maybeMary.isDefined) println(maybeMary.get)

    val marioOrNot = people.find(_ == "Mario").getOrElse("No employee with given name found :(")
    println(marioOrNot)

    // try/catch

    import scala.util.{Try, Success, Failure}

    val outcome = Try(10 / 0)
    outcome match {
        case Success(value) => println("Good!")
        case Failure(exception) => println("Bad :(")
    }

    // either

    def stringToInt(in: String): Either[String, Int] = {
        try {
            Right(in.toInt)
        } catch {
            case e: NumberFormatException => Left("Error: " + e.getMessage)
        }
    }

    println(stringToInt("5"), stringToInt("Hello!"))


    // ########################
    // ##  PATTERN MATCHING  ##
    // ########################

    val number = 6

    number match {
        case 0 => "zero"
        case 1 => "one"
        case 5 => "five"
        case _ => "nothing matched"
    }

    case class Car(brand: String, weight: Double)

    val audi = new Car("Audi", 120.0)
    val ford = new Car("Ford", 100.0)

    val c = ford match {
        case Car(brand, _) => brand
        case _ => "Car not found"
    }
    println(c)

    // lists

    val nums = List(1,2,3)
    nums match {
        case List(_, second, _) => second
        case _ => -1
    }

    case class Trip(to: String)
    case class Vehicle(model: String)
    case class Cash(amount: Integer)
    case class NoPrize()

    val magicBucket = List(NoPrize(), Vehicle("Tesla"), NoPrize(), Trip("Amsterdam"), NoPrize(), NoPrize(), Cash(2000), NoPrize())

    import scala.util.Random
    val item = Random.shuffle(magicBucket).take(1)(0)
    item match {
        case t: Trip => println("You won a trip to " + t.to)
        case v: Vehicle => println("You won a car of model " + v.model)
        case c: Cash => println("You won " + c.amount + " in cash")
        case n: NoPrize => println("You didn't win anything today :(")
    }

    case class Email(from: String, body: String)

    val importantPeople = Set("wife@home.com", "boss@office.com")

    def alertOrNah(email: Email) = email match {
        case Email(from, _) if importantPeople .contains(from) => println("This email needs your attention")
        case Email(_, _) => println("do not disturb")
    }

    alertOrNah(Email("wife@home.com", ""))
    alertOrNah(Email("oyvind@hauge.com", ""))


    // ###################
    // ##  COLLECTIONS  ##
    // ###################

    val numz = List(1,2,3,4)

    // get first element and the rest of the list
    println(numz.head, numz.tail)

    // prepend to list
    val newList = 0 +: numz
    println(newList)

    // append to list
    var newList2 = newList :+ 5
    println(newList2)

    // Note! there's also drop(n) and dropRight(n)
    // Note #2! there's also min, max sum and product which can be used directly on a numeric list

    val greaterThan = (n: Int) => n > 3
    // drops the first element that matches the predicate
    val newList3 = newList2.dropWhile(greaterThan)
    println(newList3)

    // appending lists
    val NewList4 = newList2 ++ newList3

    // maps

    val weekdays = Map(1 -> "monday", 2 -> "tuesday")

    // add to map
    var newMap = weekdays + (3 -> "wednesday")

    // remove from map
    newMap = weekdays - 1 // use the key

    val list2 = List(List(1,2,3), List(1,2,3), List(1,2,3))
    val list3 = list2.map(aList => aList.map(_ + 1)).flatten
    println(list3)

    // ...or use flatMap which is a combination of map and flatten
    println(list2.flatMap(aList => aList.map(_ + 1)))

    // flatmap with option values:

    def toInt(s: String): Option[Int] = {
        try {
            Some(s.toInt)
        } catch {
            case e: NumberFormatException => None
        }
    }

    val argss: List[String] = List("10", "eight", "1", "one", "2")
    println(argss.flatMap(toInt).sum)


    // ###################
    // ##  CONCURRENCY  ##
    // ###################


}
