package com.spotify.ratatool.describe

import cats.kernel.Semigroup
import cats.kernel.instances.map._
import io.circe.{Encoder, Json}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificData
import org.apache.avro.util.Utf8
import io.circe.syntax._

import scala.collection.JavaConverters._


sealed trait Stats

object Stats {
  implicit val statsSemigroup: Semigroup[Stats] = new Semigroup[Stats] {
    override def combine(l: Stats, r: Stats): Stats = (l, r) match {
      case (l: Record, r: Record) => Semigroup[Record].combine(l, r)
      case (l: Numerical, r: Numerical) => Semigroup[Numerical].combine(l, r)
      case (l: Textual, r: Textual) => Semigroup[Textual].combine(l, r)
      case _ => sys.error("Can not combine dissimilar objects")
    }
  }

  implicit val statsEncoder: Encoder[Stats] = new Encoder[Stats] {
    override def apply(a: Stats): Json = a match {
      case r : Record => r.asJson
      case n: Numerical => n.asJson
      case t: Textual =>t.asJson
    }
  }

  def fromField(genericRecord: GenericRecord, field: Schema.Field): Option[Stats] = {
    val name = field.name()
    val value = genericRecord.get(name)
    val valueOpt = Option(value)
    val schema = field.schema()

    resolveUnion(value, schema) match {
      case Type.RECORD =>
        val gr = valueOpt.map(_.asInstanceOf[GenericRecord])
        Some(Record.fromGenericRecord(name, gr))

      case Type.INT =>
        val l = valueOpt.map(_.asInstanceOf[Int].toLong)
        Some(Numerical.fromValue(name, l))

      case Type.LONG =>
        val l = valueOpt.map(_.asInstanceOf[Long])
        Some(Numerical.fromValue(name, l))

      case Type.STRING =>
        val s = valueOpt.map(_.asInstanceOf[Utf8].toString)
        Some(Textual.fromValue(name, s))

      case _ =>
        None
    }
  }

  /**
   * @return the first proper type of the union, useful for [null, X]
   */
  private def resolveUnion(obj: Object, schema: Schema): Schema.Type = {
    schema.getType match {
      case Type.UNION =>
        val nonNullTypes = schema.getTypes.asScala.toList.filterNot(_.getType == Type.NULL)

        nonNullTypes match {
          case List(x) => x.getType
          case _ => sys.error(s"Complex union types `${schema.getTypes}` are not supported")
        }
      case _ =>
        schema.getType
    }
  }
}

case class Record(
  name: String,
  // map is used to simplify merging
  fields: Option[Record.RecordMap],
  count: Count
) extends Stats

object Record {
  type RecordMap = Map[String, Stats]

  implicit val recordSemigroup: Semigroup[Record] = new Semigroup[Record] {
    override def combine(x: Record, y: Record): Record = {
      assert(x.name == y.name, s"`${x.name}` <> `${y.name}` can only combine same fields")

      val fieldMap = (x.fields, y.fields) match {
        case (Some(x), Some(y)) => Some(Semigroup[RecordMap].combine(x, y))
        case (Some(x), None) => Some(x)
        case (None, Some(y)) => Some(y)
        case (None, None) => None
      }

      Record(
        name = x.name,
        fields = fieldMap,
        count = Semigroup[Count].combine(x.count, y.count)
      )
    }
  }

  implicit val recordEncoder: Encoder[Record] = new Encoder[Record] {
    override def apply(a: Record): Json = Json.obj(
      "type" -> "record".asJson,
      "name" -> a.name.asJson,
      "is_nullable" -> a.count.isNullable.asJson,
      "count" -> a.count.asJson,
      "fields" -> a.fields.map(_.values).asJson
    )
  }

  def fromGenericRecord(genericRecord: GenericRecord): Record = {
    val name = genericRecord.getSchema.getFullName
    fromGenericRecord(name, Some(genericRecord))
  }

  def fromGenericRecord(name: String, genericRecord: Option[GenericRecord]): Record = {
    // silently filters out unparsable fields
    val fieldMap = genericRecord.map { gr =>
      gr.getSchema.getFields.asScala.flatMap { f =>
        Stats.fromField(gr, f).map(f.name -> _)
      }.toMap
    }

     Record(
      name = name,
      fields = fieldMap,
      count = Count.fromOption(genericRecord)
    )
  }
}

case class Numerical(
  name: String,
  count: Count,
  factor: Factor[Option[Long]],
  minMax: MinMaxAvg
) extends Stats

object Numerical {
  type NumericalFactor = Factor[Option[Long]]
  implicit val numericalFactorSemigroup: Factor.FactorSemigroup[Option[Long]] =
    new Factor.FactorSemigroup[Option[Long]]
  implicit val numericalFactorEncoder: Factor.FactorEncoder[Option[Long]] =
    new Factor.FactorEncoder[Option[Long]]

  implicit val numericalSemigroup: Semigroup[Numerical] = new Semigroup[Numerical] {
    override def combine(x: Numerical, y: Numerical): Numerical = {
      assert(x.name == y.name, s"`${x.name}` <> `${y.name}` can only combine same fields")

      Numerical(
        name = x.name,
        minMax = Semigroup[MinMaxAvg].combine(x.minMax, y.minMax),
        factor = Semigroup[NumericalFactor].combine(x.factor, y.factor),
        count = Semigroup[Count].combine(x.count, y.count)
      )
    }
  }

  implicit val numericalEncoder: Encoder[Numerical] = new Encoder[Numerical] {
    override def apply(a: Numerical): Json = Json.obj(
      "type" -> "numerical".asJson,
      "name" -> a.name.asJson,
      "is_nullable" -> a.count.isNullable.asJson,
      "count" -> a.count.asJson,
      "factor" -> a.factor.asJson,
      "min_max" -> a.minMax.asJson
    )
  }

  def fromValue(name: String, value: Option[Long]): Numerical = {
    Numerical(
      name = name,
      count = Count.fromOption(value),
      factor = Factor.fromValue(value),
      minMax = MinMaxAvg.fromLong(value)
    )
  }
}

case class Textual(
  name: String,
  count: Count,
  factor: Textual.StringFactor,
  length: MinMaxAvg
) extends Stats

object Textual {
  type StringFactor = Factor[Option[String]]
  implicit val stringFactorSemigroup: Factor.FactorSemigroup[Option[String]] =
    new Factor.FactorSemigroup[Option[String]]
  implicit val stringFactorEncoder: Factor.FactorEncoder[Option[String]] =
    new Factor.FactorEncoder[Option[String]]

  implicit val textualSemigroup: Semigroup[Textual] = new Semigroup[Textual] {
    override def combine(x: Textual, y: Textual): Textual = {
      assert(x.name == y.name, s"`${x.name}` <> `${y.name}` can only combine same fields")

      Textual(
        name = x.name,
        count = Semigroup[Count].combine(x.count, y.count),
        factor = Semigroup[StringFactor].combine(x.factor, y.factor),
        length = Semigroup[MinMaxAvg].combine(x.length, y.length)
      )
    }
  }

  implicit val textualEncoder: Encoder[Textual] = new Encoder[Textual] {
    override def apply(a: Textual): Json = Json.obj(
      "type" -> "textual".asJson,
      "name" -> a.name.asJson,
      "is_nullable" -> a.count.isNullable.asJson,
      "count" -> a.count.asJson,
      "factor" -> a.factor.asJson,
      "length" -> a.length.asJson
    )
  }

  def fromValue(name: String, value: Option[String]): Textual = {
    Textual(
      name = name,
      count = Count.fromOption(value),
      factor = Factor.fromValue(value),
      length = MinMaxAvg.fromLong(value.map(_.length.toLong))
    )
  }
}
