package com.spotify.ratatool

import com.spotify.ratatool.diffy.{Delta, Diffy}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}
import shapeless.labelled.FieldType

package object implicits {
  trait MapEncoder[A] {
    def toMap(in: A): Map[String, Any]
  }

  object MapEncoder {
    def apply[A](implicit encoder: MapEncoder[A]): MapEncoder[A] =
      encoder

    def createEndcoder[A](func: A => Map[String, Any]): MapEncoder[A] = new MapEncoder[A] {
      override def toMap(in: A): Map[String, Any] = func(in)
    }

    implicit def stringEndCoder[K <: Symbol](implicit witness: Witness.Aux[K]):
    MapEncoder[FieldType[K, String]] = {
      val name = witness.value.name
      createEndcoder { v =>
        Map(name -> v.self)
      }
    }

    implicit def intEndCoder[K <: Symbol](implicit witness: Witness.Aux[K]):
    MapEncoder[FieldType[K, Int]] = {
      val name = witness.value.name
      createEndcoder { v =>
        Map(name -> v.self)
      }
    }

    implicit def longEndCoder[K <: Symbol](implicit witness: Witness.Aux[K]):
    MapEncoder[FieldType[K, Long]] = {
      val name = witness.value.name
      createEndcoder { v =>
        Map(name -> v.self)
      }
    }

    implicit def doubleEndCoder[K <: Symbol](implicit witness: Witness.Aux[K]):
    MapEncoder[FieldType[K, Double]] = {
      val name = witness.value.name
      createEndcoder { v =>
        Map(name -> v.self)
      }
    }

    implicit val hnilEnCoder: MapEncoder[HNil] =
      createEndcoder[HNil](n => Map(HNil.toString -> HNil))

    implicit def listEncoder[K <: Symbol, V](implicit witness: Witness.Aux[K]):
    MapEncoder[FieldType[K, Seq[V]]] = {
      import scala.collection.JavaConverters._
      val name = witness.value.name
      createEndcoder { v =>
        Map(name -> v.toIndexedSeq.asJava)
      }
    }

    //scalastyle:off
    implicit def hlistEndcoder0[K <: Symbol, H, T <: HList](implicit hEncoder: Lazy[MapEncoder[FieldType[K, H]]],
                                                            tEncoder: Lazy[MapEncoder[T]]): MapEncoder[FieldType[K, H] :: T] = {
      createEndcoder[FieldType[K, H] :: T](in => hEncoder.value.toMap(in.head) ++ tEncoder.value.toMap(in.tail))
    }

    implicit def hListEndcoder1[K <: Symbol, H, T <: HList, R <: HList](implicit
                                                                        wit: Witness.Aux[K],
                                                                        gen: LabelledGeneric.Aux[H, R],
                                                                        encoderH: Lazy[MapEncoder[R]],
                                                                        encoderT: Lazy[MapEncoder[T]]
                                                                       ): MapEncoder[FieldType[K, H] :: T] =
      createEndcoder(in =>
        encoderT.value.toMap(in.tail) ++ Map(wit.value.name -> encoderH.value.toMap(gen.to(in.head))))

    implicit def genericEncoder[A, R](implicit gen: LabelledGeneric.Aux[A, R],
                                      enc: MapEncoder[R]): MapEncoder[A] = {
      createEndcoder(a => enc.toMap(gen.to(a)))
    }
  }


  implicit class ToMapOps[T](val in: T) extends AnyVal {
      def asMap(implicit diff: MapEncoder[T]) = diff.toMap(in)
  }

  class CaseClassDiffy[T](ignore: Set[String] = Set.empty,
                          unordered: Set[String] = Set.empty)(implicit diff: MapEncoder[T])
    extends Diffy[T](ignore, unordered) {

    private def hnilIgnore = ignore ++ Set(HNil.toString)

    override def apply(x: T, y: T): Seq[Delta] = {
      diff(Some(x.asMap), Some(y.asMap))
        .filter(f => !hnilIgnore.exists(f.field.contains(_)))
    }

    private def diff(left: Any, right: Any, pref: String = "")
    : Seq[Delta] = {
      def diffMap(left: Map[String, Any], right: Map[String, Any], pref: String = pref) = {
        (left.keySet ++ right.keySet).toSeq
          .flatMap(k => diff(left.get(k), right.get(k), if (pref.isEmpty) k else s"${pref}.${k}"))
      }

      (left, right) match {
        case (Some(l: Map[String, Any]), Some(r: Map[String, Any])) => diffMap(l, r, pref)
        case (Some(l), Some(r)) => Seq(Delta(pref, l, r, delta(l, r)))
      }
    }
  }
}
