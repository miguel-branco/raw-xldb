package tutorial

import scala.virtualization.lms.common._
import scala.virtualization.lms.util.OverloadHack

//Trait acting as 'interface' of functions and types that we want to lift
trait RawInterface { this: Base with OverloadHack =>

  object ColumnValue { def apply(x: Rep[String]) = column_new(x) }
  object RecordValue {
    def apply(x: Rep[List[ColumnValue]]) = record_new(x)

  }

  //Methods concerning Columns

  //Plain comparison between integers. Should define similar ones over Numerics too

  //Aliases of functions defined. In most trivial cases, their names could be plain characters representing operations,
  //such as + , / , < etc
  def gt_int(v: Rep[ColumnValue], k: Rep[ColumnValue])(implicit o: Overloaded1): Rep[Boolean] = column_gt_int(v, k)
  def gt_int(x: Rep[ColumnValue], y: Rep[Int])(implicit o: Overloaded2): Rep[Boolean] = column_gt_intRight(x, y)
  def gt_int(v: Rep[Int], k: Rep[ColumnValue])(implicit o: Overloaded3): Rep[Boolean] = column_gt_intLeft(v, k)
  def gt_int(x: Rep[Int], y: Rep[Int])(implicit o: Overloaded4): Rep[Boolean] = column_gt_intBoth(x, y)

  def column_new(x: Rep[String]): Rep[ColumnValue]

  def column_gt_int(v: Rep[ColumnValue], k: Rep[ColumnValue]): Rep[Boolean]
  def column_gt_intRight(v: Rep[ColumnValue], k: Rep[Int]): Rep[Boolean]
  def column_gt_intLeft(v: Rep[Int], k: Rep[ColumnValue]): Rep[Boolean]
  def column_gt_intBoth(v: Rep[Int], k: Rep[Int]): Rep[Boolean]

  //Redundant method used to check rewrite capabilities of LMS.
  //Will attempt something more sophisticated once issue with unnecessary object creations has been tackled
  def pos_int(v: Rep[ColumnValue]): Rep[Boolean] = column_positive_int(v)
  def column_positive_int(v: Rep[ColumnValue]): Rep[Boolean]

  //Methods concerning Records.

  //Implicit re-mapping
  implicit def repRecordToRecOps(a: Rep[RecordValue]) = new RecOpsCls(a)

  class RecOpsCls(a: Rep[RecordValue]) {
    def apply(n: Rep[Int]) = record_apply(a, n)
  }

  def record_new(x: Rep[List[ColumnValue]]): Rep[RecordValue]
  def record_apply(x: Rep[RecordValue], n: Rep[Int]): Rep[ColumnValue]

  def record_getColumns(x: Rep[RecordValue]): Rep[List[ColumnValue]]
  def getCols(v: Rep[RecordValue]): Rep[List[ColumnValue]] = record_getColumns(v)

}

/**
 * Intermediate - more concrete - representation
 * Every function assigned / matched with a specific class
 */
trait RawExp extends RawInterface with EffectExp { this: BaseExp =>

  //Columns
  case class ColumnNew(x: Exp[String]) extends Def[ColumnValue]
  def column_new(x: Exp[String]) = ColumnNew(x)

  //Comparison variants
  case class ColumnGtInt(v: Exp[ColumnValue], k: Exp[ColumnValue]) extends Def[Boolean]
  override def column_gt_int(v: Rep[ColumnValue], k: Rep[ColumnValue]) = ColumnGtInt(v, k)

  case class ColumnGtIntRight(v: Exp[ColumnValue], k: Exp[Int]) extends Def[Boolean]
  override def column_gt_intRight(v: Exp[ColumnValue], k: Exp[Int]) = ColumnGtIntRight(v, k)

  case class ColumnGtIntLeft(v: Exp[Int], k: Exp[ColumnValue]) extends Def[Boolean]
  override def column_gt_intLeft(v: Exp[Int], k: Exp[ColumnValue]) = ColumnGtIntLeft(v, k)

  case class ColumnGtIntBoth(v: Exp[Int], k: Exp[Int]) extends Def[Boolean]
  override def column_gt_intBoth(v: Exp[Int], k: Exp[Int]) = ColumnGtIntBoth(v, k)

  case class ColumnGtPositive(v: Exp[ColumnValue]) extends Def[Boolean]
  override def column_positive_int(v: Exp[ColumnValue]) = ColumnGtPositive(v)


  //Records
  case class RecordNew(x: Exp[List[ColumnValue]]) extends Def[RecordValue]
  def record_new(x: Exp[List[ColumnValue]]) = RecordNew(x)

  case class RecordApply(x: Exp[RecordValue], n: Exp[Int]) extends Def[ColumnValue]
  def record_apply(x: Exp[RecordValue], n: Exp[Int]): Exp[ColumnValue] = RecordApply(x, n)

  case class RecordColumns(x: Exp[RecordValue]) extends Def[List[ColumnValue]]
  override def record_getColumns(x: Rep[RecordValue]): Rep[List[ColumnValue]] = RecordColumns(x)

}

/**
 * Optimizations working on the intermediate representation.
 * Pattern matching utilized to simplify expressions
 */
trait RawExpOpt extends RawExp { this: BaseExp =>

//  override def column_gt_int(v: Exp[ColumnValue], k: Exp[ColumnValue]) = v match {
//
//    case _ => println("Debug: v:"+v+" and k: "+k) ; super.column_gt_int(v,k)
//  }

  //This work could probably have been done in other places too.
  //Only reason we do it here is to test the functionality offered
  override def column_positive_int(v: Exp[ColumnValue]) = v match
  {
    case _ => /*println("Overriding!!") ; */super.column_gt_intRight(v,Const(0))
  }

//  override def vector_scale(v: Exp[Vector], k: Exp[Double]) = k match {
//    case Const(1.0) => v
//    case _ => super.vector_scale(v, k)
//  }
  
}

/**
 * Trait involved with code generation
 * In this example, code is generated in Scala ("ScalaCodeGenPkg")
 */
trait ScalaGenLinearAlgebra extends ScalaCodeGenPkg with ScalaGenListOps with ScalaGenStreamOps with ScalaGenTraversableOps {

  val IR: RawExp with ScalaOpsPkgExp with StreamOpsExp with TraversableOpsExp
  import IR._

  //Note: quote function return's a symbol's unique id
  //Overriding handling of the classes we defined in RawExp, and dictating what code should be printed / generated
  override def emitNode(sym: Sym[Any], node: Def[Any]): Unit = node match {
    //Our datatypes. In our next steps we will attempt ,  through use of unit(), to avoid re-creating them (if possible)
    case ColumnNew(m) => emitValDef(sym, "tutorial.ColumnImpl(" + quote(m) + ")")
    case RecordNew(m) => emitValDef(sym, "tutorial.RecordImpl(" + quote(m) + ")")

    //Columns
    //Combinations for Integers comparison
    case ColumnGtInt(v, k) => emitValDef(sym, quote(v) + ".value.toInt > " + quote(k) + ".value.toInt")
    case ColumnGtIntRight(v, k) => emitValDef(sym, quote(v) + ".value.toInt > " + quote(k))
    case ColumnGtIntLeft(v, k) => emitValDef(sym, quote(v) + " > " + quote(k) + ".value.toInt")
    case ColumnGtIntBoth(v, k) => emitValDef(sym, quote(v) + " > " + quote(k))

    //Records
    case RecordApply(x, n) => emitValDef(sym, "" + quote(x) + ".columns(" + quote(n) + ")")
    case RecordColumns(m) => emitValDef(sym, quote(m) + ".columns")
    case _ => super.emitNode(sym, node)
  }

}