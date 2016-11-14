// package frameless

// import frameless.functions.aggregate._
// import org.scalacheck.Prop
// import org.scalacheck.Prop._
// import org.scalatest.Matchers

// class SchemaTests extends TypedDatasetSuite with Matchers {

//   def prop[A](dataset: TypedDataset[A]): Prop = {
//     TypedExpressionEncoder.targetStructType(dataset.encoder) ?= dataset.dataset.schema
//   }

//   test("schema of groupBy('a).agg(sum('b))") {
//     val df0 = TypedDataset.create(X2(1L, 1L) :: Nil)
//     val _a = df0.col('a)
//     val _b = df0.col('b)

//     val df = df0.groupBy(_a).agg(sum(_b))

//     check(prop(df))
//   }

//   test("schema of select(lit(1L))") {
//     val df0 = TypedDataset.create("test" :: Nil)
//     val df = df0.select(lit(1L))

//     check(prop(df))
//   }

//   test("schema of select(lit(1L), lit(2L)).as[X2[Long, Long]]") {
//     val df0 = TypedDataset.create("test" :: Nil)
//     val df = df0.select(lit(1L), lit(2L)).as[X2[Long, Long]]

//     check(prop(df))
//   }
// }
