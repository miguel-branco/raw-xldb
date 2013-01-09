package tutorial

import scala.virtualization.lms.common._
import java.io.PrintWriter
import scala.virtualization.lms.internal.GenericNestedCodegen
import scala.reflect.SourceContext
import sun.awt.Symbol


//Interface
trait StreamOps extends Variables {

  //Gathering symbols so that I can perform a swap later on. More complex structure will be required if we do
  //opt for such an approach
  var potentialPermutations : List[Rep[Any]] = List()


  object Stream {
    def apply[A:Manifest](xs: Rep[A]*)(implicit pos: SourceContext) = stream_new(xs)
  }

  implicit def varToStreamOps[T:Manifest](x: Var[Stream[T]]) = new StreamOpsCls(readVar(x)) // FIXME: dep on var is not nice
  implicit def repToStreamOps[T:Manifest](a: Rep[Stream[T]]) = new StreamOpsCls(a)
  implicit def streamToStreamOps[T:Manifest](a: Stream[T]) = new StreamOpsCls(unit(a))

  
  class StreamOpsCls[A:Manifest](l: Rep[Traversable[A]]) {
    def map[B:Manifest](f: Rep[A] => Rep[B]) = stream_map(l,f)
    def flatMap[B : Manifest](f: Rep[A] => Rep[Traversable[B]]) = stream_flatMap(f)(l)
    //Not withFilter, but still seemed to work OK for lists
    def filter(f: Rep[A] => Rep[Boolean]) = stream_filter(l, f)
    def head = stream_head(l)
    
    //Don't think it will ever be triggered. The function with the same name declared in Traversable probably shadows it
    //The reason is that the majority of Stream functions now reader Traversable objects
    def toList = stream_toList(l)
  }
  
  def stream_new[A:Manifest](xs: Seq[Rep[A]])(implicit pos: SourceContext): Rep[Stream[A]]
  def stream_map[A:Manifest,B:Manifest](l: Rep[Traversable[A]], f: Rep[A] => Rep[B])(implicit pos: SourceContext): Rep[Traversable[B]]
  def stream_flatMap[A : Manifest, B : Manifest](f: Rep[A] => Rep[Traversable[B]])(xs: Rep[Traversable[A]])(implicit pos: SourceContext): Rep[Traversable[B]]
  def stream_filter[A : Manifest](l: Rep[Traversable[A]], f: Rep[A] => Rep[Boolean])(implicit pos: SourceContext): Rep[Traversable[A]]
  def stream_head[A:Manifest](xs: Rep[Traversable[A]])(implicit pos: SourceContext): Rep[A]
  
  def stream_toList[A:Manifest](xs: Rep[Traversable[A]])(implicit pos: SourceContext): Rep[List[A]]
}



//Implementation
trait StreamOpsExp extends StreamOps with EffectExp with VariablesExp {
  case class StreamNew[A:Manifest](xs: Seq[Rep[A]]) extends Def[Stream[A]]
  case class StreamFromSeq[A:Manifest](xs: Rep[Seq[A]]) extends Def[Stream[A]]
  case class StreamMap[A:Manifest,B:Manifest](l: Exp[Traversable[A]], x: Sym[A], block: Block[B]) extends Def[Traversable[B]]
  case class StreamFlatMap[A:Manifest, B:Manifest](l: Exp[Traversable[A]], x: Sym[A], block: Block[Traversable[B]]) extends Def[Traversable[B]]
  case class StreamFilter[A : Manifest](l: Exp[Traversable[A]], x: Sym[A], block: Block[Boolean]) extends Def[Traversable[A]]
  case class StreamHead[A:Manifest](xs: Rep[Traversable[A]]) extends Def[A]
  case class StreamToList[A:Manifest](xs: Rep[Traversable[A]]) extends Def[List[A]]
  
  def stream_new[A:Manifest](xs: Seq[Rep[A]])(implicit pos: SourceContext) = StreamNew(xs)
  def stream_fromseq[A:Manifest](xs: Rep[Seq[A]])(implicit pos: SourceContext) = StreamFromSeq(xs)
  def stream_toList[A:Manifest](xs: Rep[Traversable[A]])(implicit pos: SourceContext) = StreamToList(xs)
  
  def stream_map[A:Manifest,B:Manifest](l: Exp[Traversable[A]], f: Exp[A] => Exp[B])(implicit pos: SourceContext) = {
    val a = fresh[A]
    val b = reifyEffects(f(a))
    reflectEffect(StreamMap(l, a, b), summarizeEffects(b).star)
  }
  
  def stream_flatMap[A:Manifest, B:Manifest](f: Exp[A] => Exp[Traversable[B]])(l: Exp[Traversable[A]])(implicit pos: SourceContext) = {
    val a = fresh[A]
    val b = reifyEffects(f(a))
    reflectEffect(StreamFlatMap(l, a, b), summarizeEffects(b).star)
  }
  
  def stream_filter[A : Manifest](l: Exp[Traversable[A]], f: Exp[A] => Exp[Boolean])(implicit pos: SourceContext) = {
    val a = fresh[A]
    val b = reifyEffects(f(a))
    reflectEffect(StreamFilter(l, a, b), summarizeEffects(b).star)
  }
  
  def stream_head[A:Manifest](xs: Rep[Traversable[A]])(implicit pos: SourceContext) = StreamHead(xs)
  
  override def mirror[A:Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = {
    (e match {
      case StreamNew(xs) => stream_new(f(xs))
      case _ => super.mirror(e,f)
    }).asInstanceOf[Exp[A]] // why??
  }
  
  //Identical methods of ListOps and ArrayOps both return Lists  - No worries, they are only involved with printing the code
  override def syms(e: Any): List[Sym[Any]] = e match {
    case StreamMap(a, x, body) => syms(a):::syms(body)
    case StreamFlatMap(a, _, body) => syms(a):::syms(body)
    //println("In the flattener " + body + " vs " + Block(globalIndex(0))) ;  syms(a) ::: syms(Block(globalIndex(1)))
    case StreamFilter(a, _, body) => syms(a) ::: syms(body)
    case _ => super.syms(e)
  }

  override def boundSyms(e: Any): List[Sym[Any]] = e match {
    case StreamMap(a, x, body) => x :: effectSyms(body)
    case StreamFlatMap(_, x, body) => x :: effectSyms(body)
    case StreamFilter(_, x, body) => x :: effectSyms(body)
    case _ => super.boundSyms(e)
  }

  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    case StreamMap(a, x, body) => freqNormal(a):::freqHot(body)
    case StreamFlatMap(a, _, body) => freqNormal(a) ::: freqHot(body)
    case StreamFilter(a, _, body) => freqNormal(a) ::: freqHot(body)
    case _ => super.symsFreq(e)
  }  
}

/**
 * Issue: de-sugared for comprehenensions include deep nestings - not straightforward to pattern match against them
 * Current 'optimizing' trait just calls default implementation
 */
trait ColumnOpt extends StreamOpsExp { this: BaseExp =>

  override def stream_flatMap[A:Manifest, B:Manifest](f: Exp[A] => Exp[Traversable[B]])(l: Exp[Traversable[A]])(implicit pos: SourceContext) = f match {

    //Default behavior
    case _ => {
      /*println("Debugging: "+f.toString+"\n"+l) ;*/
      var result = super.stream_flatMap(f)(l)
      potentialPermutations = result :: potentialPermutations
      result
    }

  }

  override def stream_map[A:Manifest,B:Manifest](l: Exp[Traversable[A]], f: Exp[A] => Exp[B])(implicit pos: SourceContext) = f match {

  case _ => {
              /*println("Debugging: "+f.toString+"\n"+l) ;*/
              var result = super.stream_map(l,f)
              potentialPermutations = result :: potentialPermutations
              result

//    case _ => {
//      println("Input: "+l)
//      val a = fresh[A]
//      println("a: "+a)
//      val tmp = f(a)
//      println("f(a): "+tmp)
//      val b = reifyEffects(f(a))
//      println("b: "+b)
//      val dunno = summarizeEffects(b).star
//      println("effects? "+dunno)
//
//      val result = reflectEffect(StreamMap(l, a, b), dunno)
//      println("result: "+result)
//      result
//    }

//    val a = fresh[A]
//    val b = reifyEffects(f(a))
//    reflectEffect(StreamMap(l, a, b), summarizeEffects(b).star)
  }
}
}


trait BaseGenStreamOps extends GenericNestedCodegen {
  val IR: StreamOpsExp
  import IR._

}

trait ScalaGenStreamOps extends BaseGenStreamOps with ScalaGenEffect {
  val IR: StreamOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case StreamNew(xs) => emitValDef(sym, "Stream(" + (xs map {quote}).mkString(",") + ")")
    case StreamHead(xs) => emitValDef(sym, quote(xs) + ".head")
    case StreamMap(l,x,blk) => 
      stream.println("val " + quote(sym) + " = " + quote(l) + ".map{")
      stream.println(quote(x) + " => ")
      emitBlock(blk)
      stream.println(quote(getBlockResult(blk)))
      stream.println("}")
    case StreamFlatMap(l, x, b) => {
      stream.println("val " + quote(sym) + " = " + quote(l) + ".flatMap { " + quote(x) + " => ")
//      println("emitting block")
//      stream.println("//")
      emitBlock(b)
//      stream.println("//")
//      println("emitting block result")
      val res = quote(getBlockResult(b))
//      println(res)
      stream.println(res)
      stream.println("}")
//      println("Did intercept these - plain: "+(potentialPermutations))
//      println("Did intercept these: "+(potentialPermutations map (zz => quote(zz))))

    }
    case StreamFilter(l, x, b) => {
      stream.println("val " + quote(sym) + " = " + quote(l) + ".filter { " + quote(x) + " => ")
      emitBlock(b)
      stream.println(quote(getBlockResult(b)))
      stream.println("}")
    }
    case StreamToList(xs) => emitValDef(sym, "" + quote(xs)+".toList")
    case _ => super.emitNode(sym, rhs)
  }
}