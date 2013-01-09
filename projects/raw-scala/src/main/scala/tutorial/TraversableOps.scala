package tutorial

import scala.virtualization.lms.common._
import java.io.PrintWriter
import scala.virtualization.lms.internal.GenericNestedCodegen
import scala.reflect.SourceContext

/**
 * As Traversable is common sub-parent of Lists and Streams, these traits had to be defined
 * to handle cases in which for comprehensions involved objects of different types
 */


//Interface
trait TraversableOps extends Variables {

  
  object Traversable {
    def apply[A:Manifest](xs: Rep[A]*)(implicit pos: SourceContext) = traversable_new(xs)
  }

  implicit def varToTraversableOps[T:Manifest](x: Var[Traversable[T]]) = new TraversableOpsCls(readVar(x)) // FIXME: dep on var is not nice
  implicit def repToTraversableOps[T:Manifest](a: Rep[Traversable[T]]) = new TraversableOpsCls(a)
  implicit def traversableToTraversableOps[T:Manifest](a: Traversable[T]) = new TraversableOpsCls(unit(a))
  
  class TraversableOpsCls[A:Manifest](l: Rep[Traversable[A]]) {
    def map[B:Manifest](f: Rep[A] => Rep[B]) = traversable_map(l,f)
    def flatMap[B : Manifest](f: Rep[A] => Rep[Traversable[B]]) = traversable_flatMap(f)(l)
    //Not withFilter, but still seemed to work OK for lists
    def filter(f: Rep[A] => Rep[Boolean]) = traversable_filter(l, f)
    def head = traversable_head(l)
    def toList = traversable_toList(l)
  }
  
  def traversable_new[A:Manifest](xs: Seq[Rep[A]])(implicit pos: SourceContext): Rep[Traversable[A]]
  def traversable_map[A:Manifest,B:Manifest](l: Rep[Traversable[A]], f: Rep[A] => Rep[B])(implicit pos: SourceContext): Rep[Traversable[B]]
  def traversable_flatMap[A : Manifest, B : Manifest](f: Rep[A] => Rep[Traversable[B]])(xs: Rep[Traversable[A]])(implicit pos: SourceContext): Rep[Traversable[B]]
  def traversable_filter[A : Manifest](l: Rep[Traversable[A]], f: Rep[A] => Rep[Boolean])(implicit pos: SourceContext): Rep[Traversable[A]]
  def traversable_head[A:Manifest](xs: Rep[Traversable[A]])(implicit pos: SourceContext): Rep[A]
  def traversable_toList[A:Manifest](xs: Rep[Traversable[A]])(implicit pos: SourceContext): Rep[List[A]]
}



//Implementation
trait TraversableOpsExp extends TraversableOps with EffectExp with VariablesExp {
  case class TraversableNew[A:Manifest](xs: Seq[Rep[A]]) extends Def[Traversable[A]]
  case class TraversableFromSeq[A:Manifest](xs: Rep[Seq[A]]) extends Def[Traversable[A]]
  case class TraversableMap[A:Manifest,B:Manifest](l: Exp[Traversable[A]], x: Sym[A], block: Block[B]) extends Def[Traversable[B]]
  case class TraversableFlatMap[A:Manifest, B:Manifest](l: Exp[Traversable[A]], x: Sym[A], block: Block[Traversable[B]]) extends Def[Traversable[B]]
  case class TraversableFilter[A : Manifest](l: Exp[Traversable[A]], x: Sym[A], block: Block[Boolean]) extends Def[Traversable[A]]
  case class TraversableHead[A:Manifest](xs: Rep[Traversable[A]]) extends Def[A]
  case class TraversableToList[A:Manifest](xs: Rep[Traversable[A]]) extends Def[List[A]]
  

  
  def traversable_new[A:Manifest](xs: Seq[Rep[A]])(implicit pos: SourceContext) = TraversableNew(xs)
  def traversable_fromseq[A:Manifest](xs: Rep[Seq[A]])(implicit pos: SourceContext) = TraversableFromSeq(xs)
  def traversable_toList[A:Manifest](xs: Rep[Traversable[A]])(implicit pos: SourceContext) = TraversableToList(xs)
  
  def traversable_map[A:Manifest,B:Manifest](l: Exp[Traversable[A]], f: Exp[A] => Exp[B])(implicit pos: SourceContext) = {
    val a = fresh[A]
    val b = reifyEffects(f(a))
    reflectEffect(TraversableMap(l, a, b), summarizeEffects(b).star)
  }
  def traversable_flatMap[A:Manifest, B:Manifest](f: Exp[A] => Exp[Traversable[B]])(l: Exp[Traversable[A]])(implicit pos: SourceContext) = {
    val a = fresh[A]
    val b = reifyEffects(f(a))
    reflectEffect(TraversableFlatMap(l, a, b), summarizeEffects(b).star)
  }
  def traversable_filter[A : Manifest](l: Exp[Traversable[A]], f: Exp[A] => Exp[Boolean])(implicit pos: SourceContext) = {
    val a = fresh[A]
    val b = reifyEffects(f(a))
    reflectEffect(TraversableFilter(l, a, b), summarizeEffects(b).star)
  }
  
  def traversable_head[A:Manifest](xs: Rep[Traversable[A]])(implicit pos: SourceContext) = TraversableHead(xs)
  
  override def mirror[A:Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = {
    (e match {
      case TraversableNew(xs) => traversable_new(f(xs))
      case _ => super.mirror(e,f)
    }).asInstanceOf[Exp[A]] // why??
  }
  
  //Identical methods of ListOps and ArrayOps both return Lists
  override def syms(e: Any): List[Sym[Any]] = e match {
    case TraversableMap(a, x, body) => syms(a):::syms(body)
    case TraversableFlatMap(a, _, body) => syms(a) ::: syms(body)
    case TraversableFilter(a, _, body) => syms(a) ::: syms(body)
    case _ => super.syms(e)
  }

  override def boundSyms(e: Any): List[Sym[Any]] = e match {
    case TraversableMap(a, x, body) => x :: effectSyms(body)
    case TraversableFlatMap(_, x, body) => x :: effectSyms(body)
    case TraversableFilter(_, x, body) => x :: effectSyms(body)
    case _ => super.boundSyms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    case TraversableMap(a, x, body) => freqNormal(a):::freqHot(body)
    case TraversableFlatMap(a, _, body) => freqNormal(a) ::: freqHot(body)
    case TraversableFilter(a, _, body) => freqNormal(a) ::: freqHot(body)
    case _ => super.symsFreq(e)
  }  
}

trait BaseGenTraversableOps extends GenericNestedCodegen {
  val IR: TraversableOpsExp
  import IR._

}


trait ScalaGenTraversableOps extends BaseGenTraversableOps with ScalaGenEffect {
  val IR: TraversableOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case TraversableNew(xs) => emitValDef(sym, "Traversable(" + (xs map {quote}).mkString(",") + ")")
    case TraversableHead(xs) => emitValDef(sym, quote(xs) + ".head")
    case TraversableMap(l,x,blk) => 
      stream.println("val " + quote(sym) + " = " + quote(l) + ".map{")
      stream.println(quote(x) + " => ")
      emitBlock(blk)
      stream.println(quote(getBlockResult(blk)))
      stream.println("}")
    case TraversableFlatMap(l, x, b) => {
      stream.println("val " + quote(sym) + " = " + quote(l) + ".flatMap { " + quote(x) + " => ")
      emitBlock(b)
      stream.println(quote(getBlockResult(b)))
      stream.println("}")
    }
    case TraversableFilter(l, x, b) => {
      stream.println("val " + quote(sym) + " = " + quote(l) + ".filter { " + quote(x) + " => ")
      emitBlock(b)
      stream.println(quote(getBlockResult(b)))
      stream.println("}")
    }
    case TraversableToList(xs) => emitValDef(sym, "" + quote(xs)+".toList")
    case _ => super.emitNode(sym, rhs)
  }
}