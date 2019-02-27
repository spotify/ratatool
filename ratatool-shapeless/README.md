# CaseClassDiffy

A tool to perform diffs on `SCollection` of case classes. `CaseClassDiffy` is defined as:

```
class CaseClassDiffy[T](ignore: Set[String] = Set.empty, unordered: Set[String] = Set.empty)

def diffCaseClass[T : ClassTag : MapEncoder](lhs: SCollection[T],
                                               rhs: SCollection[T],
                                               keyFn: T => String,
                                               diffy: CaseClassDiffy[T])
```

## Usage

Add the following dependency to your `sbt` build file:

```
libraryDependencies += "com.spotify" %% "ratatool-shapeless" % "0.3.10"
```

## CaseClassDiffy Examples 

```
import com.spotify.ratatool.shapeless.CaseClassDiffy._

case class Foo(f1: String, f2: Int, f3: Long, f4: Seq[Double], f5: Seq[String])
case class Bar(b1: Double, b2: Foo)

def avroToCaseClassFn(r: SpecificRecordClass) : Bar = ...
def keyFn(b: Bar) : String = ...

val left : SCollection[Bar] = sc.avroFile[SpecificRecordClass]("gs://..")
  .map(avroToCaseClassFn(_))

val right : SColection[Bar] = sc.avroFile[SpecificRecordClass]("gs://..")
  .map(avroToCaseClassFn(_))

diffCaseClass[Bar](left, right, keyFn, new CaseClassDiffy[Bar])
```

## Supported Types

Case classes consists of the following types and nested case classes are supported.

- Long 
- Int
- String
- Boolean
- Float
- Double
- Seq
- List
- Option

For other types, consider contributing your own encoder [here](CaseClassDiffy.scala) or create an issue.

## License

Copyright 2016-2018 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0