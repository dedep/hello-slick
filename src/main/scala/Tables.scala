import scala.slick.driver.H2Driver.simple._
import scala.slick.lifted.{ProvenShape, ForeignKeyQuery}

// A Suppliers table with 6 columns: id, name, street, city, state, zip
class Suppliers(tag: Tag)
  extends Table[Supplier](tag, "SUPPLIERS") {

  // This is the primary key column:
  def id: Column[Int] = column[Int]("SUP_ID", O.PrimaryKey)
  def name: Column[String] = column[String]("SUP_NAME")
  def street: Column[String] = column[String]("STREET")
  def city: Column[String] = column[String]("CITY")
  def state: Column[String] = column[String]("STATE")
  def zip: Column[String] = column[String]("ZIP")
  
  // Every table needs a * projection with the same type as the table's type parameter
  def * = (id, name, street, city, state, zip) <> (Supplier.tupled, Supplier.unapply)
}

// A Coffees table with 5 columns: name, supplier id, price, sales, total
class Coffees(tag: Tag)
  extends Table[Coffee](tag, "COFFEES") {

  def name: Column[String] = column[String]("COF_NAME", O.PrimaryKey)
  def supID: Column[Int] = column[Int]("SUP_ID")
  def price: Column[Double] = column[Double]("PRICE")
  def sales: Column[Int] = column[Int]("SALES")
  def total: Column[Int] = column[Int]("TOTAL")
  
  def * = (name, supID, price, sales, total) <> (Coffee.tupled, Coffee.unapply)
  
  // A reified foreign key relation that can be navigated to create a join
  def supplier: ForeignKeyQuery[Suppliers, Supplier] =
    foreignKey("SUP_FK", supID, TableQuery[Suppliers])(_.id)
}
