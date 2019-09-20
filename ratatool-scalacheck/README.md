Generators
=======

Ratatool-Scalacheck contains classes and functions which help with using Scalacheck generators with
 Avro, Protobuf, and BigQuery. For examples of using Generators see [ratatool-examples](https://github.com/spotify/ratatool/tree/master/ratatool-examples).

## Usage
```scala
import com.spotify.ratatool.scalacheck._
import org.scalacheck.Gen


val avroGen: Gen[MyRecord] = avroOf[MyRecord]
val record: MyRecord = avroGen.sample.get
```

Ratatool also provides `protobufOf[T]` and `tableRowOf(schema)` defined [here](https://github.com/spotify/ratatool/tree/master/ratatool-scalacheck/src/main/scala/com/spotify/ratatool/scalacheck).

It also enables modifying specific fields using `.amend()`. For Protobuf, the record has to be
 converted to a Builder first.

```scala
val intGen = Gen.choose(0, 10)
avroGen.amend(intGen)(_.setMyIntField)
```

To use the same value in two records, use `amend2()` after converting `(Gen[A], Gen[B])` to
 `Gen[(A, B)]` with `tupled()`.
 
 ```scala
val otherGen: Gen[OtherRecord] = avroOf[OtherRecord]
val keyGen = Arbitrary.arbString.arbitrary

(avroGen, otherGen).tupled.amend2(keyGen)(_.setMyIntField, _.setOtherIntField)
```

Implicit Arbitrary instances are also available for Avro and Protobuf records. Explicit functions
 are provided for GenericRecord and TableRow

```scala
val avroArb: Arbitrary[MyRecord] = implicitly[Arbitrary[MyRecord]]

```
## CaseClassGenerator

Given a case class containing fields, and (potentially non-arbitrary) generators for individual 
fields in the implicit scope, generates a Gen for the case class. 

Inspired by and code based on what 
[Scalacheck-Magnolia](https://github.com/mrdziuban/scalacheck-magnolia) does for Arbitrary. 


Note that this works with Scio 0.7.x and earlier, due to [this Magnolia change](https://github.com/propensive/magnolia/pull/152) added to Scio [here](https://github.com/spotify/scio/pull/2241/).
You may see a `java.lang.NoSuchMethodError: magnolia.TypeName.<init>` if you're using an 
incompatible Scio version. 

```scala
import com.spotify.ratatool.scalacheck.CaseClassGenerator._
import org.scalacheck.Gen

case class Sample(opt: Option[String])

object OptionGen {
  implicit def optionAlwaysSome[T](implicit genT: Gen[T]): Gen[Option[T]] = genT.map(Some(_))
}

object ValidCaseClassGen {
  import OptionGen.optionAlwaysSome
  implicit def string: Gen[String] = Gen.alphaNumStr
  def sampleGen: Gen[Sample] = deriveGen[Sample] // always gens a Sample(Some(alphaNumString))
}
```
