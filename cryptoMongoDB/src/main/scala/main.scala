import org.bson.BsonDocument
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.Projections.{excludeId, fields, include}
import org.mongodb.scala.model.Sorts.{ascending, descending, orderBy}
import org.mongodb.scala.model.Filters._
import spray.json.DefaultJsonProtocol.{StringJsonFormat, immSeqFormat, listFormat, tuple2Format}
import spray.json._

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.reflect.io._


//case class Cryptocurrence(name: String, ticker: String,price: Double, h24: Double,
//                          h7: Double, market_cap: Long, volume: Long, circulating_supply: Long)
//
//
//object MyJsonProtocol extends DefaultJsonProtocol {
//  implicit val cryptocurrenceFormat = jsonFormat8(Cryptocurrence)
//}
//import MyJsonProtocol._

object Helpers {

  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
    def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results().foreach(res => println(converter(res)))
    }
    def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
  }

}
import Helpers._
//import MyJsonProtocol._
object main {


  def main(arr:Array[String]): Unit ={

    val pathJSON ="src/main/resources/output/"
    val mongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("test")
    val coll: MongoCollection[Document] = database.getCollection("crypto")
    coll.drop().results()


    importData(pathJSON,coll)
    first5cap(coll)
//    price_large1000(coll)
//    minus_changes_all_day(coll)
//    small_volume(coll)
//    price_gt_eth(coll)
  }

  def importData(pathJSON: String, coll:MongoCollection[Document]): Unit ={
    val path = Paths.get(pathJSON)
    val file = path.toFile
    if(file.isDirectory){
      val Str = file.listFiles().filter(_.getName.endsWith(".json")).map(_.toString).toList
      for(el<- Str){
        val info = Document(Source.fromFile(el).mkString)
        println(info)
        coll.insertOne(info).results()
      }
    }
  }


  def first5cap(coll:MongoCollection[Document]): Unit ={
    val sm =coll
      .find()
      .projection(fields(include("name","market_cap"),excludeId()))
      .sort(orderBy(descending("market_cap")))
      .limit(5)
      .results()
    println(sm)
    val q = sm.map(_.toList)
    val list_name = for (el <- q ) yield el(1)._2.asString().getValue
    val list_price = for (el <- q ) yield el(0)._2.asInt64().getValue
    val list = list_name.zip(list_price)
    println("Вывести первые 5 криптовалют по капитализации")
    list.foreach(println)
  }

  def price_large1000(coll:MongoCollection[Document]): Unit ={
    val sm =coll
      .find(gt("price",1000))
      .sort(orderBy(descending("price")))
      .projection(fields(include("name","price"),excludeId()))
      .results()
    val q = sm.map(_.toList)
    val list_name = for (el <- q ) yield el(0)._2.asString().getValue
    val list_price = for (el <- q ) yield el(1)._2.asDouble().getValue
    val list = list_name.zip(list_price)
    println("Вывести криптовалюты, где стоимость> 1000")
    list.foreach(println)
  }

  def minus_changes_all_day(coll:MongoCollection[Document]): Unit ={
    val sm =coll
      .find(and(lt("h24",0),lt("h7",0)))
      .sort(orderBy(ascending("name")))
      .projection(fields(include("name","h24","h7"),excludeId()))
      .results()
    val q = sm.map(_.toList)
    val list_h24 = for (el <- q ) yield el(0)._2.asDouble().getValue
    val list_h7 = for (el <- q ) yield el(1)._2.asDouble().getValue
    val list_name = for (el <- q ) yield el(2)._2.asString().getValue
    val list = (for(i <- 1 to list_name.size-1) yield (list_name(i),list_h24(i),list_h7(i))).toList
    println("Вывести криптовалюты, у которых “24h” <0 и “7h”<0")
    list.foreach(println)
  }


  def small_volume(coll:MongoCollection[Document]): Unit ={
    val sm =coll
      .find(gt("volume",0))
      .sort(orderBy(ascending("volume")))
      .projection(fields(include("name","volume"),excludeId()))
      .first()
      .headResult()
    val q = sm.toList
    val list_name = q(0)._2.asString().getValue
    val list_volume = q(1)._2.asInt32().getValue
    val list = (list_name, list_volume)
    println("Вывести криптовалюту с наименьшим объемом")
    println(list)
  }

  def price_gt_eth(coll:MongoCollection[Document]): Unit ={
    val price=coll
      .find(equal("name","Ethereum"))
      .projection(fields(include("price"),excludeId()))
      .first()
      .headResult()
    val priceEth = price.toList.map(_._2.asDouble().getValue)(0)

    val sm=coll
      .find(gt("price",priceEth))
      .sort(orderBy(descending("price")))
      .projection(fields(include("name","price"),excludeId()))
      .results()
    val q = sm.map(_.toList)
    val list_name = for (el <- q ) yield el(0)._2.asString().getValue
    val list_price = for (el <- q ) yield el(1)._2.asDouble().getValue
    val list = list_name.zip(list_price)
    println("Вывести криптовалюты, у которых стоимость> стоимости у Ethereum")
    list.foreach(println)
  }


}
