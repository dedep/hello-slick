import org.scalatest._
import scala.slick.driver.H2Driver.simple._
import scala.slick.jdbc.meta._
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

class TablesSuite extends FunSuite with BeforeAndAfter {

  val suppliers = TableQuery[Suppliers]
  val coffees = TableQuery[Coffees]
  
  implicit var session: Session = _

  def createSchema() = (suppliers.ddl ++ coffees.ddl).create

  def insertSuppliers() = {
    suppliers ++= Seq(
      Supplier(101, "Acme, Inc.", "99 Market Street", "Groundsville", "CA", "95199"),
      Supplier(102, "Flaf, Inc.", "1 Market Street", "Hammersfield", "ON", "12342"),
      Supplier(103, "Pepco, Inc.", "13 Cross", "LA", "CA", "93762"),
      Supplier(104, "SunDream", "65 Trade Square", "New York", "NY", "13262"),
      Supplier(105, "Sisters Inc.", "15 Trade Square", "New York", "NY", "14262")
    )
  }

  def insertCoffees() = {
    coffees ++= Seq (
      Coffee("Colombian",          101, 7.99, 0, 0),
      Coffee("French_Roast",       101, 8.99, 0, 0),
      Coffee("Espresso",           103, 9.99, 0, 0),
      Coffee("Colombian_Decaf",    103, 8.99, 0, 0),
      Coffee("French_Roast_Decaf", 105, 9.99, 0, 0)
    )
  }
  
  before {
    session = Database.forURL("jdbc:h2:mem:test1", driver = "org.h2.Driver").createSession()
  }
  
  test("Creating the Schema works") {
    createSchema()
    
    val tables = MTable.getTables().list()

    assert(tables.size == 2)
    assert(tables.count(_.name.name.equalsIgnoreCase("suppliers")) == 1)
    assert(tables.count(_.name.name.equalsIgnoreCase("coffees")) == 1)
  }
  
  test("Query Suppliers works") {
    createSchema()
    insertSuppliers()

    val results = suppliers.list()

    assert(results.size == 5)
  }

  test("Test selecting special columns") {
    createSchema()
    insertSuppliers()
    val query = suppliers.map(s => (s.city, s.state))

    val results = query.list()

    assert(results(0).isInstanceOf[(String, String)])
    assert(results.size == 5)
  }

  test("Suppliers filtering works") {
    createSchema()
    insertSuppliers()

    val result = suppliers.filter(_.name === "Flaf, Inc.").firstOption

    assert(result.isDefined)
    assert(result.get.name === "Flaf, Inc.")
  }

  test("For-comprehensive Suppliers filtering works") {
    createSchema()
    insertSuppliers()

    val result = (for (sup <- suppliers if sup.name === "Flaf, Inc.") yield sup).firstOption

    assert(result.isDefined)
    assert(result.get.name === "Flaf, Inc.")
  }

  test("Suppliers sorting works") {
    createSchema()
    insertSuppliers()

    val results = suppliers.sortBy(_.zip.asc).list

    assert(results.head.zip === "12342")
    assert(results.last.zip === "95199")
  }

  test("Suppliers and Coffees Cross Join works") {
    createSchema()
    insertSuppliers()
    insertCoffees()

    val results = (for (sup <- suppliers ; cof <- coffees) yield (sup, cof)).list

    assert(results.size === 25)
  }

  test("Suppliers and Coffees Inner Join works") {
    createSchema()
    insertSuppliers()
    insertCoffees()

    val results = (for (sup <- suppliers ; cof <- coffees if cof.supID === sup.id) yield (sup, cof)).list

    assert(results.size === 5)
  }

  test("Union queries works") {
    createSchema()
    insertSuppliers()

    val result1 = suppliers.filter(_.name === "Flaf, Inc.")
    val result2 = suppliers.filter(_.zip === "14262")

    val result = (result1 union result2).list

    assert(result.size === 2)
  }

  test("Min function works") {
    createSchema()
    insertSuppliers()
    insertCoffees()

    val result = Query(coffees.map(_.price).min).first.get

    assert(result === 7.99)
  }

  test("Max function works") {
    createSchema()
    insertSuppliers()
    insertCoffees()

    val result = Query(coffees.map(_.price).max).first.get

    assert(result === 9.99)
  }

  test("Zipping works") {
    createSchema()
    insertSuppliers()
    insertCoffees()

    val result = (suppliers zip coffees).list

    assert(result.size === 5)
    assert(result.head._1.id === 101 && result.head._2.name === "Colombian")
  }

  test("Generated SQL example") {
    createSchema()
    insertSuppliers()

    val statement = suppliers.sortBy(_.zip.asc)

    assert(statement.selectStatement === "select x2.\"SUP_ID\", x2.\"SUP_NAME\", x2.\"STREET\", x2.\"CITY\", x2.\"STATE\", x2.\"ZIP\" " +
      "from \"SUPPLIERS\" x2 order by x2.\"ZIP\"")
  }

  test("Coffee updating works") {
    createSchema()
    insertSuppliers()
    insertCoffees()

    coffees.filter(_.name === "Colombian").update(Coffee("Colombian-Updated", 103, 8.99, 0, 0))

    assert(coffees.filter(_.name === "Colombian-Updated").firstOption.nonEmpty)
  }

  test("Coffee deleting works") {
    createSchema()
    insertSuppliers()
    insertCoffees()

    coffees.filter(_.name === "Colombian").delete

    assert(coffees.list.size === 4)
    assert(coffees.filter(_.name === "Colombian").firstOption.isEmpty)
  }

  test("IN subquery test") {
    createSchema()
    insertSuppliers()
    insertCoffees()
    val subqueryNames = coffees.filter(_.price < 9d).map(_.name)
    val fullQuery = coffees.filter(_.name in subqueryNames)

    val result = fullQuery.run

    assert(result.size == 3)
  }

  test("Scalar value subquery") {
    createSchema()
    insertSuppliers()
    insertCoffees()
    val rand = SimpleFunction.nullary[Double]("RAND") // This method returns a double between 0 (including) and 1 (excluding)
    val rndPriceQuery = (coffees.map(_.price).max.asColumnOf[Double] * rand).asColumnOf[Double]

    val result = coffees.filter(_.price > rndPriceQuery).sortBy(_.price).list

    assert(result.nonEmpty)
  }

  test("Plain SQL query test") {
    createSchema()
    insertSuppliers()
    insertCoffees()

    implicit val getSupplierResult = GetResult(r => Supplier(r.nextInt, r.nextString, r.nextString,
      r.nextString, r.nextString, r.nextString))
    implicit val getCoffeeResult = GetResult(r => Coffee(r.<<, r.<<, r.<<, r.<<, r.<<))

    Q.queryNA[Coffee]("select * from coffees") foreach { c =>
      println("  " + c.name + "\t" + c.supId + "\t" + c.price + "\t" + c.sales + "\t" + c.total)
    }
  }
  
  after {
    session.close()
  }

}