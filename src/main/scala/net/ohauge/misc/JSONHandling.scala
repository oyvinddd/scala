package net.ohauge.misc

import org.apache.spark.sql.{Encoders, SparkSession}
import net.ohauge.spark.Utils._
import sttp.client3._

import java.sql.Timestamp

case class ContactDetails(firstName: Option[String], lastName: Option[String], address1: Option[String], address2: Option[String],
                          address3: Option[String], zip: Option[String], country: Option[String], phone: Option[String],
                          email: Option[String], company: Option[String], nationality: Option[String])

case class Data(reference: String, totalSalesPriceIncVat: Option[Double], totalSalesPriceExVat: Option[Double], totalSalesVat: Option[Double],
                totalProfit: Option[Double], totalExternalCommission: Option[Double], totalMarkup: Option[Double], paymentRef: Option[String],
                paymentFee: Option[Double], orderStatus: Option[String], productSalesChannel: Option[ProductSalesChannel], contactDetails: Option[ContactDetails],
                orderLines: Option[Seq[OrderLines]], passengers: Option[Seq[Passengers]], createdDate: Option[Timestamp],
                updatedDate: Option[Timestamp])

case class OrderLineDetails(id: Option[Int], externalInstanceId: Option[String], externalProductKey: Option[String],
                            externalSourceSystem: Option[String], externalSupplier: Option[String], externalStatus: Option[String],
                            externalProductName: Option[String], externalBookingRef: Option[String], externalCommission: Option[Double],
                            inventoryReservationId: Option[String], mandatory: Option[Boolean], salesPriceIncVat: Option[Double],
                            salesPriceExVat: Option[Double], totalSalesVat: Option[Double], start: Option[String], end: Option[String],
                            orderLineDetailsPrices: Seq[OrderLineDetailsPrices], profit: Option[Double], markup: Option[Double])

case class OrderLineDetailsPrices(vatRateDescription: Option[String], priceType: Option[String], totalAmountIncVat: Option[Double],
                                  totalAmountExVat: Option[Double], vatRate: Option[Double], totalVatAmount: Option[Double],
                                  ticketType: Option[String], ticketQuantity: Option[Int], refund: Option[String])

case class OrderLines(id: Option[Int], productPackageVersionId: Option[String], productId: Option[Int], productName: Option[String],
                      salesPriceIncVat: Option[Double], salesPriceExVat: Option[Double], totalSalesVat: Option[Double],
                      totalProfit: Option[Double], totalExternalCommission: Option[Double], totalMarkup: Option[Double],
                      orderLineDetails: Option[Seq[OrderLineDetails]])

case class Passengers(firstName: Option[String], lastName: Option[String], dateOfBirth: Option[Timestamp], nationality: Option[String],
                      passportNumber: Option[String], age: Option[Int])

case class ProductSalesChannel(id: Option[Int], periodName: Option[String], fromDate: Option[Timestamp], toDate: Option[Timestamp],
                               productSalesChannelCategories: Option[Seq[String]], salesChannel: Option[String])

case class RootInterface(pageNumber: Option[Int], pageSize: Option[Int], firstPage: Option[String], lastPage: Option[String],
                         totalPages: Option[Int], totalRecords: Option[Int], nextPage: Option[String], previousPage: Option[String],
                         data: Option[Seq[Data]])

object SparkJSONHandling extends App {

    val spark = SparkSession.builder().master("local[1]").appName("SparkJSONHandling").getOrCreate()
    import spark.implicits._

    val df = spark.read
      .option("multiline", true)
      .schema(Encoders.product[RootInterface].schema)
      .json("./response2.json")
      .as[RootInterface]
      .toDF()

    val orderDF = df.transform(structColumnToDataFrame("data", isList = true))

    val contactDF = orderDF.transform(structColumnToDataFrame("contactDetails", Some("reference")))

    orderDF.transform(structColumnToDataFrame("contactDetails", Some("reference"), Some("order_ref"))).show()

    val orderLinesDF = orderDF.transform(structColumnToDataFrame("orderLines", Some("reference"), isList = true))

    val orderLineDetailsDF = orderLinesDF.transform(structColumnToDataFrame("orderLineDetails", Some("id"), Some("orderLine_id"), isList = true))

    val pricesDF = orderLineDetailsDF.transform(structColumnToDataFrame("orderLineDetailsPrices", isList = true))

    val productSalesChannelDF = orderDF.transform(structColumnToDataFrame("productSalesChannel"))

    orderDF.show()
    contactDF.show()
    orderLinesDF.show()
    orderLineDetailsDF.show()
    productSalesChannelDF.show()
}
