package com.spotify.ratatool.scalacheck

import magnolia.{CaseClass, Magnolia, SealedTrait, Subtype}
import org.scalacheck.Gen

import scala.language.experimental.macros

/** Based on Scalacheck-Magnolia, but with specific Gen that's in scope instead of Arbitrary
  * https://github.com/mrdziuban/scalacheck-magnolia
  */
object CaseClassGenerator {
  type Typeclass[A] = Gen[A]

  def combine[A](cc: CaseClass[Gen, A]): Gen[A] =
    cc.parameters.zipWithIndex.foldRight(Gen.const(List[Any]())) {
      case ((param, idx), acc) =>
      Gen.sized { size =>
        if (size < 0) {
          for {
            head <- Gen.resize(size, Gen.lzy(param.typeclass))
            tail <- Gen.resize(size, Gen.lzy(acc))
          } yield head :: tail
        } else {
          val n = cc.parameters.length - (idx + 1)
          val remainder = size % (n + 1)
          val fromRemainderGen = if (remainder > 0) {
            Gen.choose(1, n).map(r => if (r <= remainder) { 1 } else { 0 } )
          } else {
            Gen.const(0)
          }
          for {
            fromRemainder <- fromRemainderGen
            headSize = size / (n + 1) + fromRemainder
            head <- Gen.resize(headSize, Gen.lzy(param.typeclass))
            tail <- Gen.resize(size - headSize, Gen.lzy(acc))
          } yield head :: tail
        }
      }
    }.map(cc.rawConstruct(_))

  def dispatch[A](st: SealedTrait[Gen, A])(): Gen[A] =
    st.subtypes.toList match {
      case Nil => Gen.fail
      case stHead :: stTail =>
        def gen(head: Subtype[Typeclass, A], tail: Seq[Subtype[Typeclass, A]]): Gen[A] =
          Gen.sized { size =>
            val nextSize = (size - 1).max(0)
            tail match {
              case Nil => Gen.resize(nextSize, Gen.lzy(head.typeclass))
              case h :: t => Gen.frequency(
                1 -> Gen.resize(nextSize, Gen.lzy(head.typeclass)),
                (t.length + 1) -> Gen.resize(nextSize, gen(h, t)))
            }
          }

        gen(stHead, stTail)
    }

  implicit def deriveGen[A]: Gen[A] = macro Magnolia.gen[A]
}