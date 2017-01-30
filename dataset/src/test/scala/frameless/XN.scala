package frameless

import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Cogen}

case class X1[A](a: A)

object X1 {
  implicit def arbitrary[A: Arbitrary]: Arbitrary[X1[A]] = {
    Arbitrary(implicitly[Arbitrary[A]].arbitrary.map(X1(_)))
  }

  implicit def ordering[A: Ordering]: Ordering[X1[A]] = Ordering[A].on(_.a)

  implicit def cogen[A](implicit A: Cogen[A]): Cogen[X1[A]] =
    Cogen((seed: Seed, o: X1[A]) => A.perturb(seed, o.a))
}

case class X2[A, B](a: A, b: B)

object X2 {
  implicit def arbitrary[A: Arbitrary, B: Arbitrary]: Arbitrary[X2[A, B]] = {
    Arbitrary(Arbitrary.arbTuple2[A, B].arbitrary.map((X2.apply[A, B] _).tupled))
  }

  implicit def ordering[A: Ordering, B: Ordering]: Ordering[X2[A, B]] = Ordering.Tuple2[A, B].on(x => (x.a, x.b))

  implicit def cogen[A, B](implicit A: Cogen[A], B: Cogen[B]): Cogen[X2[A, B]] =
    Cogen((seed: Seed, o: X2[A, B]) => B.perturb(A.perturb(seed, o.a), o.b))
}

case class X3[A, B, C](a: A, b: B, c: C)

object X3 {
  implicit def arbitrary[A: Arbitrary, B: Arbitrary, C: Arbitrary]: Arbitrary[X3[A, B, C]] = {
    Arbitrary(Arbitrary.arbTuple3[A, B, C].arbitrary.map((X3.apply[A, B, C] _).tupled))
  }

  implicit def ordering[A: Ordering, B: Ordering, C: Ordering]: Ordering[X3[A, B, C]] =
    Ordering.Tuple3[A, B, C].on(x => (x.a, x.b, x.c))

  implicit def cogen[A, B, C](implicit A: Cogen[A], B: Cogen[B], C: Cogen[C]): Cogen[X3[A, B, C]] =
    Cogen((seed: Seed, o: X3[A, B, C]) => C.perturb(B.perturb(A.perturb(seed, o.a), o.b), o.c))
}

case class X4[A, B, C, D](a: A, b: B, c: C, d: D)

object X4 {
  implicit def arbitrary[A: Arbitrary, B: Arbitrary, C: Arbitrary, D: Arbitrary]: Arbitrary[X4[A, B, C, D]] = {
    Arbitrary(Arbitrary.arbTuple4[A, B, C, D].arbitrary.map((X4.apply[A, B, C, D] _).tupled))
  }

  implicit def ordering[A: Ordering, B: Ordering, C: Ordering, D: Ordering]: Ordering[X4[A, B, C, D]] =
    Ordering.Tuple4[A, B, C, D].on(x => (x.a, x.b, x.c, x.d))

  implicit def cogen[A, B, C, D]
  (implicit
   A: Cogen[A], B: Cogen[B], C: Cogen[C], D: Cogen[D]): Cogen[X4[A, B, C, D]] =
    Cogen((seed: Seed, o: X4[A, B, C, D]) =>
      D.perturb(C.perturb(B.perturb(A.perturb(seed, o.a), o.b), o.c), o.d))
}

case class X5[A, B, C, D, E](a: A, b: B, c: C, d: D, e: E)

object X5 {
  implicit def arbitrary[A: Arbitrary, B: Arbitrary, C: Arbitrary, D: Arbitrary, E: Arbitrary]: Arbitrary[X5[A, B, C, D, E]] = {
    Arbitrary(Arbitrary.arbTuple5[A, B, C, D, E].arbitrary.map((X5.apply[A, B, C, D, E] _).tupled))
  }

  implicit def ordering[A: Ordering, B: Ordering, C: Ordering, D: Ordering, E: Ordering]: Ordering[X5[A, B, C, D, E]] =
    Ordering.Tuple5[A, B, C, D, E].on(x => (x.a, x.b, x.c, x.d, x.e))

  implicit def cogen[A, B, C, D, E]
  (implicit
   A: Cogen[A], B: Cogen[B], C: Cogen[C], D: Cogen[D], E: Cogen[E]): Cogen[X5[A, B, C, D, E]] =
    Cogen((seed: Seed, o: X5[A, B, C, D, E]) =>
      E.perturb(D.perturb(C.perturb(B.perturb(A.perturb(seed, o.a), o.b), o.c), o.d), o.e))
}
