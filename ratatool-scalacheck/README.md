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
