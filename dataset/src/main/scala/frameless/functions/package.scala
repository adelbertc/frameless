package frameless

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{ col => sparkCol }
import shapeless.Witness

package object functions extends Udf with UnaryFunctions {
  object aggregate extends AggregateFunctions
  object nonAggregate extends NonAggregateFunctions

  /** Creates a [[frameless.TypedAggregate]] of literal value. If A is to be encoded using an Injection make
    * sure the injection instance is in scope.
    *
    * apache/spark
    */
  def litAggr[A: TypedEncoder, T](value: A): TypedAggregate[T, A] =
    new TypedAggregate[T,A](lit(value).expr)


  /** Creates a [[frameless.TypedColumn]] of literal value. If A is to be encoded using an Injection make
    * sure the injection instance is in scope.
    *
    * apache/spark
    */
  def lit[A: TypedEncoder, T](value: A): TypedColumn[T, A] = {
    val encoder = TypedEncoder[A]

    if (ScalaReflection.isNativeType(encoder.jvmRepr) && encoder.catalystRepr == encoder.jvmRepr) {
      val expr = Literal(value, encoder.catalystRepr)
      new TypedColumn(expr)
    } else {
      val expr = FramelessLit(value, encoder)
      new TypedColumn(expr)
    }
  }

  def col[T, A](column: Witness.Lt[Symbol])(
    implicit
    exists: TypedColumn.Exists[T, column.T, A],
    encoder: TypedEncoder[A]): TypedColumn[T, A] = {
    val untypedExpr = sparkCol(column.value.name).as[A](TypedExpressionEncoder[A])
    new TypedColumn[T, A](untypedExpr)
  }
}
