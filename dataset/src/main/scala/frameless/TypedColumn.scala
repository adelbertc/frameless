package frameless

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{Column, FramelessInternals}
import shapeless.ops.record.Selector
import shapeless._

import scala.annotation.implicitNotFound

sealed trait UntypedExpression {
  def expr: Expression
}

/** Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  */
sealed class TypedColumn[U](val expr: Expression)(implicit val uencoder: TypedEncoder[U]) extends UntypedExpression {

  /** From an untyped Column to a [[TypedColumn]]
    *
    * @param column a spark.sql Column
    * @param uencoder encoder of the resulting type U
    */
  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  /** Fall back to an untyped Column
    */
  def untyped: Column = new Column(expr)

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === 1 )
    * }}}
    *
    * apache/spark
    */
  def ===(other: U): TypedColumn[Boolean] = {
    new TypedColumn[Boolean](untyped === other)
  }

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def ===(other: TypedColumn[U]): TypedColumn[Boolean] = {
    new TypedColumn[Boolean](untyped === other.untyped)
  }

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) plus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def plus(u: TypedColumn[U])(implicit n: CatalystNumeric[U]): TypedColumn[U] =
    new TypedColumn[U](untyped.plus(u.untyped))

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) + people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def +(u: TypedColumn[U])(implicit n: CatalystNumeric[U]): TypedColumn[U] = plus(u)

  /** Sum of this expression (column) with a constant.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people('height) + 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def +(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[U] = new TypedColumn[U](untyped.plus(u))

  /** Unary minus, i.e. negate the expression.
    * {{{
    *   // Select the amount column and negates all values.
    *   df.select( -df('amount) )
    * }}}
    *
    * apache/spark
    */
  def unary_-(implicit n: CatalystNumeric[U]): TypedColumn[U] = new TypedColumn[U](-untyped)


  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) minus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def minus(u: TypedColumn[U])(implicit n: CatalystNumeric[U]): TypedColumn[U] =
    new TypedColumn[U](untyped.minus(u.untyped))

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) - people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def -(u: TypedColumn[U])(implicit n: CatalystNumeric[U]): TypedColumn[U] = minus(u)

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people('height) - 1 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def -(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[U] = new TypedColumn[U](untyped.minus(u))

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) multiply people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def multiply(u: TypedColumn[U])(implicit n: CatalystNumeric[U]): TypedColumn[U] =
    new TypedColumn[U](untyped.multiply(u.untyped))

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *(u: TypedColumn[U])(implicit n: CatalystNumeric[U]): TypedColumn[U] = multiply(u)

  /** Multiplication of this expression a constant.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[U] = new TypedColumn[U](untyped.multiply(u))

  /**
    * Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / people('weight) )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def divide(u: TypedColumn[U])(implicit n: CatalystNumeric[U]): TypedColumn[Double] = new TypedColumn[Double](untyped.divide(u.untyped))

  /**
    * Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / people('weight) )
    * }}}
    *
    * @param u another column of the same type
    * apache/spark
    */
  def /(u: TypedColumn[U])(implicit n: CatalystNumeric[U]): TypedColumn[Double] = divide(u)

  /**
    * Division this expression by another expression.
    * {{{
    *   // The following divides a person's height by their weight.
    *   people.select( people('height) / 2 )
    * }}}
    *
    * @param u a constant of the same type
    * apache/spark
    */
  def /(u: U)(implicit n: CatalystNumeric[U]): TypedColumn[Double] = new TypedColumn[Double](untyped.divide(u))

  /** Casts the column to a different type.
    * {{{
    *   df.select(df('a).cast[Int])
    * }}}
    */
  def cast[A: TypedEncoder](implicit c: CatalystCast[U, A]): TypedColumn[A] =
    new TypedColumn(untyped.cast(TypedEncoder[A].targetDataType))
}

sealed trait TypedAggregate[A] extends UntypedExpression {
  def expr: Expression
  def aencoder: TypedEncoder[A]
}

sealed class TypedAggregateAndColumn[A, U](expr: Expression)(
  implicit
  val aencoder: TypedEncoder[A],
  uencoder: TypedEncoder[U]
) extends TypedColumn[U](expr) with TypedAggregate[A] {

  def this(column: Column)(implicit aencoder: TypedEncoder[A], uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }
}

object TypedColumn {
  /**
    * Evidence that type `T` has column `K` with type `V`.
    */
  @implicitNotFound(msg = "No column ${K} of type ${V} in ${T}")
  trait Exists[T, K, V]

  @implicitNotFound(msg = "No columns ${K} of type ${V} in ${T}")
  trait ExistsMany[T, K <: HList, V]

  object ExistsMany {
    implicit def deriveCons[T, KH, KT <: HList, V0, V1](
      implicit
      head: Exists[T, KH, V0],
      tail: ExistsMany[V0, KT, V1]
    ): ExistsMany[T, KH :: KT, V1] = new ExistsMany[T, KH :: KT, V1] {}

    implicit def deriveHNil[T, K, V](
      implicit
      head: Exists[T, K, V]
    ): ExistsMany[T, K :: HNil, V] = new ExistsMany[T, K :: HNil, V] {}
  }

  object Exists {
    def apply[T, V](column: Witness)(
      implicit
      exists: Exists[T, column.T, V]
    ): Exists[T, column.T, V] = exists

    implicit def deriveRecord[T, H <: HList, K, V](
      implicit
      lgen: LabelledGeneric.Aux[T, H],
      selector: Selector.Aux[H, K, V]
    ): Exists[T, K, V] = new Exists[T, K, V] {}
  }
}
