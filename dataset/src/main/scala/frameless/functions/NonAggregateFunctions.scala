package frameless
package functions

import org.apache.spark.sql.{Column, functions => untyped}

import scala.util.matching.Regex

trait NonAggregateFunctions {
  /** Non-Aggregate function: returns the absolute value of a numeric column
    *
    * apache/spark
    */
  def abs[A, B, T](column: AbstractTypedColumn[T, A])
    (implicit
      i0: CatalystAbsolute[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
      column.typed(untyped.abs(column.untyped))(i1)

  /** Non-Aggregate function: Computes the cosine of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def cos[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.cos(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the hyperbolic cosine of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def cosh[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.cosh(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the sine of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def sin[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.sin(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the hyperbolic sine of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def sinh[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.sinh(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the tangent of the given column.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def tan[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.tan(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the hyperbolic tangent of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def tanh[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.tanh(column.cast[Double].untyped))

  /** Non-Aggregate function: returns the acos of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def acos[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.acos(column.cast[Double].untyped))

  /** Non-Aggregate function: returns true if value is contained with in the array in the specified column
    *
    * apache/spark
    */
  def arrayContains[C[_]: CatalystCollection, A, T](column: AbstractTypedColumn[T, C[A]], value: A): column.ThisType[T, Boolean] =
    column.typed(untyped.array_contains(column.untyped, value))

  /** Non-Aggregate function: returns the atan of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def atan[A, T](column: AbstractTypedColumn[T,A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.atan(column.cast[Double].untyped))

  /** Non-Aggregate function: returns the asin of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def asin[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(untyped.asin(column.cast[Double].untyped))

  /** Non-Aggregate function: returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def atan2[A, B, T](l: TypedColumn[T, A], r: TypedColumn[T, B])
    (implicit
      i0: CatalystCast[A, Double],
      i1: CatalystCast[B, Double]
    ): TypedColumn[T, Double] =
      r.typed(untyped.atan2(l.cast[Double].untyped, r.cast[Double].untyped))

  /** Non-Aggregate function: returns the angle theta from the conversion of rectangular coordinates (x, y) to
    * polar coordinates (r, theta).
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def atan2[A, B, T](l: TypedAggregate[T, A], r: TypedAggregate[T, B])
    (implicit
      i0: CatalystCast[A, Double],
      i1: CatalystCast[B, Double]
    ): TypedAggregate[T, Double] =
      r.typed(untyped.atan2(l.cast[Double].untyped, r.cast[Double].untyped))

  def atan2[B, T](l: Double, r: TypedColumn[T, B])
    (implicit i0: CatalystCast[B, Double]): TypedColumn[T, Double] =
      atan2(r.lit(l), r)

  def atan2[A, T](l: TypedColumn[T, A], r: Double)
    (implicit i0: CatalystCast[A, Double]): TypedColumn[T, Double] =
      atan2(l, l.lit(r))

  def atan2[B, T](l: Double, r: TypedAggregate[T, B])
    (implicit i0: CatalystCast[B, Double]): TypedAggregate[T, Double] =
      atan2(r.lit(l), r)

  def atan2[A, T](l: TypedAggregate[T, A], r: Double)
    (implicit i0: CatalystCast[A, Double]): TypedAggregate[T, Double] =
      atan2(l, l.lit(r))

  /** Non-Aggregate function: Returns the string representation of the binary value of the given long
    * column. For example, bin("12") returns "1100".
    *
    * apache/spark
    */
  def bin[T](column: AbstractTypedColumn[T, Long]): column.ThisType[T, String] =
    column.typed(untyped.bin(column.untyped))

  /** Non-Aggregate function: Computes bitwise NOT.
    *
    * apache/spark
    */
  def bitwiseNOT[A: CatalystBitwise, T](column: AbstractTypedColumn[T, A]): column.ThisType[T, A] =
    column.typed(untyped.bitwiseNOT(column.untyped))(column.uencoder)

  /** Non-Aggregate function: file name of the current Spark task. Empty string if row did not originate from
    * a file
    *
    * apache/spark
    */
  def inputFileName[T](): TypedColumn[T, String] = {
    new TypedColumn[T, String](untyped.input_file_name())
  }

  /** Non-Aggregate function: generates monotonically increasing id
    *
    * apache/spark
    */
  def monotonicallyIncreasingId[T](): TypedColumn[T, Long] = {
    new TypedColumn[T, Long](untyped.monotonically_increasing_id())
  }

  /** Non-Aggregate function: Evaluates a list of conditions and returns one of multiple
    * possible result expressions. If none match, otherwise is returned
    * {{{
    *   when(ds('boolField), ds('a))
    *     .when(ds('otherBoolField), lit(123))
    *     .otherwise(ds('b))
    * }}}
    * apache/spark
    */
  def when[T, A](condition: AbstractTypedColumn[T, Boolean], value: AbstractTypedColumn[T, A]): When[T, A] =
    new When[T, A](condition, value)

  class When[T, A] private (untypedC: Column) {
    private[functions] def this(condition: AbstractTypedColumn[T, Boolean], value: AbstractTypedColumn[T, A]) =
      this(untyped.when(condition.untyped, value.untyped))

    def when(condition: AbstractTypedColumn[T, Boolean], value: AbstractTypedColumn[T, A]): When[T, A] =
      new When[T, A](untypedC.when(condition.untyped, value.untyped))

    def otherwise(value: AbstractTypedColumn[T, A]): value.ThisType[T, A] =
      value.typed(untypedC.otherwise(value.untyped))(value.uencoder)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // String functions
  //////////////////////////////////////////////////////////////////////////////////////////////


  /** Non-Aggregate function: takes the first letter of a string column and returns the ascii int value in a new column
    *
    * apache/spark
    */
  def ascii[T](column: AbstractTypedColumn[T, String]): column.ThisType[T, Int] =
    column.typed(untyped.ascii(column.untyped))

  /** Non-Aggregate function: Computes the BASE64 encoding of a binary column and returns it as a string column.
    * This is the reverse of unbase64.
    *
    * apache/spark
    */
  def base64[T](column: AbstractTypedColumn[T, Array[Byte]]): column.ThisType[T, String] =
    column.typed(untyped.base64(column.untyped))

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column.
    * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
    *
    * apache/spark
    */
  def concat[T](columns: TypedColumn[T, String]*): TypedColumn[T, String] =
    new TypedColumn(untyped.concat(columns.map(_.untyped): _*))

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column.
    * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
    *
    * apache/spark
    */
  def concat[T](columns: TypedAggregate[T, String]*): TypedAggregate[T, String] =
    new TypedAggregate(untyped.concat(columns.map(_.untyped): _*))

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column,
    * using the given separator.
    * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
    *
    * apache/spark
    */
  def concatWs[T](sep: String, columns: TypedAggregate[T, String]*): TypedAggregate[T, String] =
    new TypedAggregate(untyped.concat_ws(sep, columns.map(_.untyped): _*))

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column,
    * using the given separator.
    * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
    *
    * apache/spark
    */
  def concatWs[T](sep: String, columns: TypedColumn[T, String]*): TypedColumn[T, String] =
    new TypedColumn(untyped.concat_ws(sep, columns.map(_.untyped): _*))

  /** Non-Aggregate function: Locates the position of the first occurrence of substring column
    * in given string
    *
    * @note The position is not zero based, but 1 based index. Returns 0 if substr
    * could not be found in str.
    *
    * apache/spark
    */
  def instr[T](str: AbstractTypedColumn[T, String], substring: String): str.ThisType[T, Int] =
    str.typed(untyped.instr(str.untyped, substring))

  /** Non-Aggregate function: Computes the length of a given string.
    *
    * apache/spark
    */
  //TODO: Also for binary
  def length[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Int] =
    str.typed(untyped.length(str.untyped))

  /** Non-Aggregate function: Computes the Levenshtein distance of the two given string columns.
    *
    * apache/spark
    */
  def levenshtein[T](l: TypedColumn[T, String], r: TypedColumn[T, String]): TypedColumn[T, Int] =
    l.typed(untyped.levenshtein(l.untyped, r.untyped))

  /** Non-Aggregate function: Computes the Levenshtein distance of the two given string columns.
    *
    * apache/spark
    */
  def levenshtein[T](l: TypedAggregate[T, String], r: TypedAggregate[T, String]): TypedAggregate[T, Int] =
    l.typed(untyped.levenshtein(l.untyped, r.untyped))

  /** Non-Aggregate function: Converts a string column to lower case.
    *
    * apache/spark
    */
  def lower[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(untyped.lower(str.untyped))

  /** Non-Aggregate function: Left-pad the string column with pad to a length of len. If the string column is longer
    * than len, the return value is shortened to len characters.
    *
    * apache/spark
    */
  def lpad[T](str: AbstractTypedColumn[T, String],
              len: Int,
              pad: String): str.ThisType[T, String] =
    str.typed(untyped.lpad(str.untyped, len, pad))

  /** Non-Aggregate function: Trim the spaces from left end for the specified string value.
    *
    * apache/spark
    */
  def ltrim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(untyped.ltrim(str.untyped))

  /** Non-Aggregate function: Replace all substrings of the specified string value that match regexp with rep.
    *
    * apache/spark
    */
  def regexpReplace[T](str: AbstractTypedColumn[T, String],
                       pattern: Regex,
                       replacement: String): str.ThisType[T, String] =
    str.typed(untyped.regexp_replace(str.untyped, pattern.regex, replacement))


  /** Non-Aggregate function: Reverses the string column and returns it as a new string column.
    *
    * apache/spark
    */
  def reverse[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(untyped.reverse(str.untyped))

  /** Non-Aggregate function: Right-pad the string column with pad to a length of len.
    * If the string column is longer than len, the return value is shortened to len characters.
    *
    * apache/spark
    */
  def rpad[T](str: AbstractTypedColumn[T, String], len: Int, pad: String): str.ThisType[T, String] =
    str.typed(untyped.rpad(str.untyped, len, pad))

  /** Non-Aggregate function: Trim the spaces from right end for the specified string value.
    *
    * apache/spark
    */
  def rtrim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(untyped.rtrim(str.untyped))

  /** Non-Aggregate function: Substring starts at `pos` and is of length `len`
    *
    * apache/spark
    */
  //TODO: Also for byte array
  def substring[T](str: AbstractTypedColumn[T, String], pos: Int, len: Int): str.ThisType[T, String] =
    str.typed(untyped.substring(str.untyped, pos, len))

  /** Non-Aggregate function: Trim the spaces from both ends for the specified string column.
    *
    * apache/spark
    */
  def trim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(untyped.trim(str.untyped))

  /** Non-Aggregate function: Converts a string column to upper case.
    *
    * apache/spark
    */
  def upper[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(untyped.upper(str.untyped))

  /** Non-Aggregate function: Extracts the year as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#year` by wrapping it's result into an `Option`.
    *
    * apache/spark
    */
  def year[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Option[Int]] =
    str.typed(untyped.year(str.untyped))

  /** Non-Aggregate function: Computes the floor of the given value.
    *
    * apache/spark
    */
  def floor[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Int] =
    col.typed(untyped.floor(col.cast[Double].untyped)) //TODO: output type

  /** Non-Aggregate function: Computes the ceiling of the given value.
    *
    * apache/spark
    */
  def ceil[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Int] =
    col.typed(untyped.ceil(col.cast[Double].untyped)) //TODO: output type

  /** Non-Aggregate function: Returns the value of the first argument raised to the power of the second argument.
    *
    * apache/spark
    */
  def pow[B, T](left: Double, right: AbstractTypedColumn[T, B])(
                 implicit
                 i1: CatalystCast[B, Double]
  ): right.ThisType[T, Double] =
    right.typed(untyped.pow(left, right.cast[Double].untyped))

  /** Non-Aggregate function: Returns the value of the first argument raised to the power of the second argument.
    *
    * apache/spark
    */
  def pow[A, T](left: AbstractTypedColumn[T, A], right: Double)(
                 implicit
                 i0: CatalystCast[A, Double]
  ): left.ThisType[T, Double] =
    left.typed(untyped.pow(left.cast[Double].untyped, right))

  /** Non-Aggregate function: Returns the value of the first argument raised to the power of the second argument.
    *
    * apache/spark
    */
  def pow[A, B, T](left: AbstractTypedColumn[T, A], right: AbstractTypedColumn[T, B])(
    implicit
      i0: CatalystCast[A, Double],
      i1: CatalystCast[B, Double]
  ): left.ThisType[T, Double] =
    left.typed(untyped.pow(left.cast[Double].untyped, right.cast[Double].untyped))

  /** Non-Aggregate function: Computes the square root of the specified float value.
    *
    * apache/spark
    */
  def sqrt[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Double] =
    col.typed(untyped.sqrt(col.cast[Double].untyped))

  /** Non-Aggregate function: Returns the double value that is closest in value to the argument and
    * is equal to a mathematical integer.
    *
    * apache/spark
    */
  def rint[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Double] =
    col.typed(untyped.rint(col.cast[Double].untyped))

  /** Non-Aggregate function: Computes the exponential of the given value.
    *
    * apache/spark
    */
  def exp[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Double] =
    col.typed(untyped.exp(col.cast[Double].untyped))

  /** Non-Aggregate function: Computes the exponential of the given value minus one.
    *
    * apache/spark
    */
  def expm1[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Double] =
    col.typed(untyped.expm1(col.cast[Double].untyped))

  /** Non-Aggregate function: Computes the natural logarithm of the given value.
    *
    * apache/spark
    */
  def log[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Double] =
    col.typed(untyped.log(col.cast[Double].untyped))

  /** Non-Aggregate function: Computes the natural logarithm of the given value plus one.
    *
    * apache/spark
    */
  def log1p[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Double] =
    col.typed(untyped.log1p(col.cast[Double].untyped))

  /** Non-Aggregate function: Computes the logarithm of the given column in base 2.
    *
    * apache/spark
    */
  def log2[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Double] =
    col.typed(untyped.log2(col.cast[Double].untyped))

  /** Non-Aggregate function: Computes the logarithm of the given value in base 10.
    *
    * apache/spark
    */
  def log10[A, T](col: AbstractTypedColumn[T, A])(implicit i0: CatalystCast[A, Double]): col.ThisType[T, Double] =
    col.typed(untyped.log10(col.cast[Double].untyped))

}
