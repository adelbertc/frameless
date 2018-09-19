package frameless
package functions

import org.apache.spark.sql.{Column, functions => sparkFunctions}

import scala.util.matching.Regex

trait NonAggregateFunctions {
  /** Non-Aggregate function: returns the ceiling of a numeric column
    *
    * apache/spark
    */
  def ceil[A, B, T](column: AbstractTypedColumn[T, A])
    (implicit
      i0: CatalystRound[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
      column.typed(sparkFunctions.ceil(column.untyped))(i1)

  /** Non-Aggregate function: unsigned shift the the given value numBits right. If given long, will return long else it will return an integer.
    *
    * apache/spark
    */
  def shiftRightUnsigned[A, B, T](column: AbstractTypedColumn[T, A], numBits: Int)
    (implicit
      i0: CatalystBitShift[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
      column.typed(sparkFunctions.shiftRightUnsigned(column.untyped, numBits))

  /** Non-Aggregate function: shift the the given value numBits right. If given long, will return long else it will return an integer.
    *
    * apache/spark
    */
  def shiftRight[A, B, T](column: AbstractTypedColumn[T, A], numBits: Int)
    (implicit
      i0: CatalystBitShift[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
      column.typed(sparkFunctions.shiftRight(column.untyped, numBits))

  /** Non-Aggregate function: shift the the given value numBits left. If given long, will return long else it will return an integer.
    *
    * apache/spark
    */
  def shiftLeft[A, B, T](column: AbstractTypedColumn[T, A], numBits: Int)
    (implicit
      i0: CatalystBitShift[A, B],
      i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
      column.typed(sparkFunctions.shiftLeft(column.untyped, numBits))

  /** Non-Aggregate function: returns the absolute value of a numeric column
    *
    * apache/spark
    */
  def abs[A, B, T](column: AbstractTypedColumn[T, A])
    (implicit
     i0: CatalystNumericWithJavaBigDecimal[A, B],
     i1: TypedEncoder[B]
    ): column.ThisType[T, B] =
      column.typed(sparkFunctions.abs(column.untyped))(i1)

  /** Non-Aggregate function: Computes the cosine of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def cos[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.cos(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the hyperbolic cosine of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def cosh[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.cosh(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the sine of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def sin[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.sin(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the hyperbolic sine of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def sinh[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.sinh(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the tangent of the given column.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def tan[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.tan(column.cast[Double].untyped))

  /** Non-Aggregate function: Computes the hyperbolic tangent of the given value.
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def tanh[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.tanh(column.cast[Double].untyped))

  /** Non-Aggregate function: returns the acos of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def acos[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.acos(column.cast[Double].untyped))

  /** Non-Aggregate function: returns true if value is contained with in the array in the specified column
    *
    * apache/spark
    */
  def arrayContains[C[_]: CatalystCollection, A, T](column: AbstractTypedColumn[T, C[A]], value: A): column.ThisType[T, Boolean] =
    column.typed(sparkFunctions.array_contains(column.untyped, value))

  /** Non-Aggregate function: returns the atan of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def atan[A, T](column: AbstractTypedColumn[T,A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.atan(column.cast[Double].untyped))

  /** Non-Aggregate function: returns the asin of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def asin[A, T](column: AbstractTypedColumn[T, A])
    (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
      column.typed(sparkFunctions.asin(column.cast[Double].untyped))

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
      r.typed(sparkFunctions.atan2(l.cast[Double].untyped, r.cast[Double].untyped))

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
      r.typed(sparkFunctions.atan2(l.cast[Double].untyped, r.cast[Double].untyped))

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

  /** Non-Aggregate function: returns the square root value of a numeric column.
    *
    * apache/spark
    */
  def sqrt[A, T](column: AbstractTypedColumn[T, A])
                (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
    column.typed(sparkFunctions.sqrt(column.cast[Double].untyped))

  /** Non-Aggregate function: returns the cubic root value of a numeric column.
    *
    * apache/spark
    */
  def cbrt[A, T](column: AbstractTypedColumn[T, A])
                (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
    column.typed(sparkFunctions.cbrt(column.cast[Double].untyped))

  /** Non-Aggregate function: returns the exponential value of a numeric column.
    *
    * apache/spark
    */
  def exp[A, T](column: AbstractTypedColumn[T, A])
               (implicit i0: CatalystCast[A, Double]): column.ThisType[T, Double] =
    column.typed(sparkFunctions.exp(column.cast[Double].untyped))

  /** Non-Aggregate function: Bankers Rounding - returns the rounded to 0 decimal places value with HALF_EVEN round mode
    *  of a numeric column.
    *
    * apache/spark
    */
  def bround[A, B, T](column: AbstractTypedColumn[T, A])(
    implicit i0: CatalystNumericWithJavaBigDecimal[A, B], i1: TypedEncoder[B]
  ): column.ThisType[T, B] =
    column.typed(sparkFunctions.bround(column.untyped))(i1)

  /** Non-Aggregate function: Bankers Rounding - returns the rounded to `scale` decimal places value with HALF_EVEN round mode
    *  of a numeric column. If `scale` is greater than or equal to 0 or at integral part when `scale` is less than 0.
    *
    * apache/spark
    */
  def bround[A, B, T](column: AbstractTypedColumn[T, A], scale: Int)(
    implicit i0: CatalystNumericWithJavaBigDecimal[A, B], i1: TypedEncoder[B]
  ): column.ThisType[T, B] =
    column.typed(sparkFunctions.bround(column.untyped, scale))(i1)


  /** Non-Aggregate function: Returns the string representation of the binary value of the given long
    * column. For example, bin("12") returns "1100".
    *
    * apache/spark
    */
  def bin[T](column: AbstractTypedColumn[T, Long]): column.ThisType[T, String] =
    column.typed(sparkFunctions.bin(column.untyped))

  /** Non-Aggregate function: Computes bitwise NOT.
    *
    * apache/spark
    */
  def bitwiseNOT[A: CatalystBitwise, T](column: AbstractTypedColumn[T, A]): column.ThisType[T, A] =
    column.typed(sparkFunctions.bitwiseNOT(column.untyped))(column.uencoder)

  /** Non-Aggregate function: file name of the current Spark task. Empty string if row did not originate from
    * a file
    *
    * apache/spark
    */
  def inputFileName[T](): TypedColumn[T, String] = {
    new TypedColumn[T, String](sparkFunctions.input_file_name())
  }

  /** Non-Aggregate function: generates monotonically increasing id
    *
    * apache/spark
    */
  def monotonicallyIncreasingId[T](): TypedColumn[T, Long] = {
    new TypedColumn[T, Long](sparkFunctions.monotonically_increasing_id())
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
      this(sparkFunctions.when(condition.untyped, value.untyped))

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
    column.typed(sparkFunctions.ascii(column.untyped))

  /** Non-Aggregate function: Computes the BASE64 encoding of a binary column and returns it as a string column.
    * This is the reverse of unbase64.
    *
    * apache/spark
    */
  def base64[T](column: AbstractTypedColumn[T, Array[Byte]]): column.ThisType[T, String] =
    column.typed(sparkFunctions.base64(column.untyped))

  /** Non-Aggregate function: Decodes a BASE64 encoded string column and returns it as a binary column.
    * This is the reverse of base64.
    *
    * apache/spark
    */
  def unbase64[T](column: AbstractTypedColumn[T, String]): column.ThisType[T, Array[Byte]] =
    column.typed(sparkFunctions.unbase64(column.untyped))

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column.
    * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
    *
    * apache/spark
    */
  def concat[T](columns: TypedColumn[T, String]*): TypedColumn[T, String] =
    new TypedColumn(sparkFunctions.concat(columns.map(_.untyped): _*))

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column.
    * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
    *
    * apache/spark
    */
  def concat[T](columns: TypedAggregate[T, String]*): TypedAggregate[T, String] =
    new TypedAggregate(sparkFunctions.concat(columns.map(_.untyped): _*))

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column,
    * using the given separator.
    * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
    *
    * apache/spark
    */
  def concatWs[T](sep: String, columns: TypedAggregate[T, String]*): TypedAggregate[T, String] =
    new TypedAggregate(sparkFunctions.concat_ws(sep, columns.map(_.untyped): _*))

  /** Non-Aggregate function: Concatenates multiple input string columns together into a single string column,
    * using the given separator.
    * @note varargs make it harder to generalize so we overload the method for [[TypedColumn]] and [[TypedAggregate]]
    *
    * apache/spark
    */
  def concatWs[T](sep: String, columns: TypedColumn[T, String]*): TypedColumn[T, String] =
    new TypedColumn(sparkFunctions.concat_ws(sep, columns.map(_.untyped): _*))

  /** Non-Aggregate function: Locates the position of the first occurrence of substring column
    * in given string
    *
    * @note The position is not zero based, but 1 based index. Returns 0 if substr
    * could not be found in str.
    *
    * apache/spark
    */
  def instr[T](str: AbstractTypedColumn[T, String], substring: String): str.ThisType[T, Int] =
    str.typed(sparkFunctions.instr(str.untyped, substring))

  /** Non-Aggregate function: Computes the length of a given string.
    *
    * apache/spark
    */
  //TODO: Also for binary
  def length[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, Int] =
    str.typed(sparkFunctions.length(str.untyped))

  /** Non-Aggregate function: Computes the Levenshtein distance of the two given string columns.
    *
    * apache/spark
    */
  def levenshtein[T](l: TypedColumn[T, String], r: TypedColumn[T, String]): TypedColumn[T, Int] =
    l.typed(sparkFunctions.levenshtein(l.untyped, r.untyped))

  /** Non-Aggregate function: Computes the Levenshtein distance of the two given string columns.
    *
    * apache/spark
    */
  def levenshtein[T](l: TypedAggregate[T, String], r: TypedAggregate[T, String]): TypedAggregate[T, Int] =
    l.typed(sparkFunctions.levenshtein(l.untyped, r.untyped))

  /** Non-Aggregate function: Converts a string column to lower case.
    *
    * apache/spark
    */
  def lower[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.lower(str.untyped))

  /** Non-Aggregate function: Left-pad the string column with pad to a length of len. If the string column is longer
    * than len, the return value is shortened to len characters.
    *
    * apache/spark
    */
  def lpad[T](str: AbstractTypedColumn[T, String],
              len: Int,
              pad: String): str.ThisType[T, String] =
    str.typed(sparkFunctions.lpad(str.untyped, len, pad))

  /** Non-Aggregate function: Trim the spaces from left end for the specified string value.
    *
    * apache/spark
    */
  def ltrim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.ltrim(str.untyped))

  /** Non-Aggregate function: Replace all substrings of the specified string value that match regexp with rep.
    *
    * apache/spark
    */
  def regexpReplace[T](str: AbstractTypedColumn[T, String],
                       pattern: Regex,
                       replacement: String): str.ThisType[T, String] =
    str.typed(sparkFunctions.regexp_replace(str.untyped, pattern.regex, replacement))


  /** Non-Aggregate function: Reverses the string column and returns it as a new string column.
    *
    * apache/spark
    */
  def reverse[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.reverse(str.untyped))

  /** Non-Aggregate function: Right-pad the string column with pad to a length of len.
    * If the string column is longer than len, the return value is shortened to len characters.
    *
    * apache/spark
    */
  def rpad[T](str: AbstractTypedColumn[T, String], len: Int, pad: String): str.ThisType[T, String] =
    str.typed(sparkFunctions.rpad(str.untyped, len, pad))

  /** Non-Aggregate function: Trim the spaces from right end for the specified string value.
    *
    * apache/spark
    */
  def rtrim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.rtrim(str.untyped))

  /** Non-Aggregate function: Substring starts at `pos` and is of length `len`
    *
    * apache/spark
    */
  //TODO: Also for byte array
  def substring[T](str: AbstractTypedColumn[T, String], pos: Int, len: Int): str.ThisType[T, String] =
    str.typed(sparkFunctions.substring(str.untyped, pos, len))

  /** Non-Aggregate function: Trim the spaces from both ends for the specified string column.
    *
    * apache/spark
    */
  def trim[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.trim(str.untyped))

  /** Non-Aggregate function: Converts a string column to upper case.
    *
    * apache/spark
    */
  def upper[T](str: AbstractTypedColumn[T, String]): str.ThisType[T, String] =
    str.typed(sparkFunctions.upper(str.untyped))

  /** Non-Aggregate function: Extracts the year as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#year` by wrapping it's result into an `Option` in case column
    * cannot be parsed into valid date.
    *
    * apache/spark
    */
  def year[T](date: AbstractTypedColumn[T, String]): date.ThisType[T, Option[Int]] =
    date.typed(sparkFunctions.year(date.untyped))

  /** Non-Aggregate function: Extracts the day of the year as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#dayofyear` by wrapping it's result into an `Option` in case the column
    * cannot be parsed into valid date.
    *
    * apache/spark
    */
  def dayofyear[T](date: AbstractTypedColumn[T, String]): date.ThisType[T, Option[Int]] =
    date.typed(sparkFunctions.dayofyear(date.untyped))

  /** Non-Aggregate function: Extracts the week number as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#weekofyear` by wrapping it's result into an `Option` in case the column
    * cannot be parsed into valid date.
    *
    * apache/spark
    */
  def weekofyear[T](date: AbstractTypedColumn[T, String]): date.ThisType[T, Option[Int]] =
    date.typed(sparkFunctions.weekofyear(date.untyped))

  /** Non-Aggregate function: Extracts the month as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#month` by wrapping it's result into an `Option` in case the column
    * cannot be parsed into valid date.
    *
    * apache/spark
    */
  def month[T](date: AbstractTypedColumn[T, String]): date.ThisType[T, Option[Int]] =
    date.typed(sparkFunctions.month(date.untyped))

  /** Non-Aggregate function: Extracts the day of the month as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#dayofmonth` by wrapping it's result into an `Option` in case the column
    * cannot be parsed into valid date.
    *
    * apache/spark
    */
  def dayofmonth[T](date: AbstractTypedColumn[T, String]): date.ThisType[T, Option[Int]] =
    date.typed(sparkFunctions.dayofmonth(date.untyped))

  /** Non-Aggregate function: Extracts the minutes as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#minute` by wrapping it's result into an `Option` in case the column
    * cannot be parsed into valid date.
    *
    * apache/spark
    */
  def minute[T](date: AbstractTypedColumn[T, String]): date.ThisType[T, Option[Int]] =
    date.typed(sparkFunctions.minute(date.untyped))

  /** Non-Aggregate function: Extracts the seconds as an integer from a given date/timestamp/string.
    *
    * Differs from `Column#second` by wrapping it's result into an `Option` in case the column
    * cannot be parsed into valid date.
    *
    * apache/spark
    */
  def second[T](date: AbstractTypedColumn[T, String]): date.ThisType[T, Option[Int]] =
    date.typed(sparkFunctions.second(date.untyped))

  /**
    * Non-Aggregate function: Given a date column, returns the first date which is later than the value
    * of the date column that is on the specified day of the week.
    *
    * For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
    * Sunday after 2015-07-27.
    *
    * Day of the week parameter is case insensitive, and accepts:
    * "Su", "Sun", "Sunday",
    * "Mo", "Mon", "Monday",
    * "Tu", "Tue", "Tuesday",
    * "We", "Wed", "Wednesday",
    * "Th", "Thu", "Thursday",
    * "Fr", "Fri", "Friday",
    * "Sa", "Sat", "Saturday".
    *
    * Differs from `Column#next_day` by wrapping it's result into an `Option` in case the column
    * cannot be parsed into valid date.
    *
    * apache/spark
    */
  def next_day[T](date: AbstractTypedColumn[T, String], dayOfWeek: String): date.ThisType[T, Option[java.sql.Date]] =
    date.typed(sparkFunctions.next_day(date.untyped, dayOfWeek))
}