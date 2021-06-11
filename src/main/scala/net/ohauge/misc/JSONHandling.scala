package net.ohauge.misc

import scala.io.Source
import io.circe.generic.auto._
import io.circe.parser._
import net.ohauge.spark.Utils._
import org.apache.spark.sql.{Encoders, SparkSession}
import sttp.client3._

case class ContactDetails(firstName: Option[String], lastName: Option[String], address1: Option[String], address2: Option[String],
                          address3: Option[String], zip: Option[String], country: Option[String], phone: Option[String],
                          email: Option[String], company: Option[String], nationality: Option[String])

case class Data(reference: String, totalSalesPriceIncVat: Option[Int], totalSalesPriceExVat: Option[Double], totalSalesVat: Option[Double],
                totalProfit: Option[Int], totalExternalCommission: Option[Int], totalMarkup: Option[Int], paymentRef: Option[String],
                orderStatus: Option[String], productSalesChannel: Option[ProductSalesChannel], contactDetails: Option[ContactDetails],
                orderLines: Option[Seq[OrderLines]], passengers: Option[Seq[Passengers]], createdDate: Option[String],
                updatedDate: Option[String])

case class OrderLineDetails(id: Option[Int], externalInstanceId: Option[String], externalProductKey: Option[String],
                            externalSourceSystem: Option[String], externalSupplier: Option[String], externalStatus: Option[String],
                            externalProductName: Option[String], externalBookingRef: Option[String], externalCommission: Option[Int],
                            inventoryReservationId: Option[String], mandatory: Option[Boolean], salesPriceIncVat: Option[Int],
                            salesPriceExVat: Option[Double], totalSalesVat: Option[Double], start: Option[String], end: Option[String],
                            orderLineDetailsPrices: Seq[OrderLineDetailsPrices], profit: Option[Int], markup: Option[Int])

case class OrderLineDetailsPrices(vatRateDescription: Option[String], priceType: Option[String], totalAmountIncVat: Option[Double],
                                  totalAmountExVat: Option[Double], vatRate: Option[Double], totalVatAmount: Option[Double],
                                  ticketType: Option[String], ticketQuantity: Option[Int], refund: Option[String])

case class OrderLines(id: Option[Int], productPackageVersionId: Option[String], productId: Option[Int], productName: Option[String],
                      salesPriceIncVat: Option[Int], salesPriceExVat: Option[Double], totalSalesVat: Option[Double],
                      totalProfit: Option[Int], totalExternalCommission: Option[Int], totalMarkup: Option[Int],
                      orderLineDetails: Option[Seq[OrderLineDetails]])

case class Passengers(firstName: Option[String], lastName: Option[String], dateOfBirth: Option[String], nationality: Option[String],
                      passportNumber: Option[String], age: Option[Int])

case class ProductSalesChannel(id: Option[Int], periodName: Option[String], fromDate: Option[String], toDate: Option[String],
                               productSalesChannelCategories: Option[Seq[String]], salesChannel: Option[String])

case class RootInterface(pageNumber: Option[Int], pageSize: Option[Int], firstPage: Option[String], lastPage: Option[String],
                         totalPages: Option[Int], totalRecords: Option[Int], nextPage: Option[String], previousPage: Option[String],
                         data: Option[Seq[Data]])

// for testing purposes only
case class Player(name: Option[String], age: Option[Int])
case class Team(name: Option[String], players: Option[Seq[Player]])

object JSONHandling extends App {
    // setup and initialize spark session object
    val spark = SparkSession.builder().master("local[1]").appName("JSONHandling").getOrCreate()
    import spark.implicits._

    val json: String = Source.fromFile("./response.json").mkString

    val decoded = decode[RootInterface](json) match {
        case Right(rootInterface) => rootInterface
        case Left(error) => error
    }

    println(decoded)
}

object SparkJSONHandling extends App {
    val spark = SparkSession.builder().master("local[1]").appName("SparkJSONHandling").getOrCreate()
    import spark.implicits._

    val request = basicRequest.get(uri"https://jsonplaceholder.typicode.com/posts")

    val backend = HttpURLConnectionBackend()
    val response = request.send(backend)
    println(response)

    val rootSchema = Encoders.product[RootInterface].schema

    val df = spark.read
      .option("multiline", true)
      .schema(rootSchema)
      .json("./response.json")
      .as[RootInterface]
      .toDF()

    val orderDF = df.transform(structListColumnToDataFrame("data"))

    val contactDF = orderDF.transform(structColumnToDataFrame("contactDetails", "reference"))

    val orderLinesDF = orderDF.transform(structListColumnToDataFrame("orderLines", "reference"))

    val orderLineDetailsDF = orderLinesDF.transform(structListColumnToDataFrame("orderLineDetails"))

    val pricesDF = orderLineDetailsDF.transform(structListColumnToDataFrame("orderLineDetailsPrices"))

    val productSalesChannelDF = orderDF.transform(structColumnToDataFrame("productSalesChannel"))



    orderDF.show()
    contactDF.show()
    orderLinesDF.show()
    orderLineDetailsDF.show()
    productSalesChannelDF.show()
}
