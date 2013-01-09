package tutorial

import scala.util.parsing.combinator._
import scala.util.parsing.input.Positional
import java.io.FileReader

object examples {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(159); 
  var test = 2;System.out.println("""test  : Int = """ + $show(test ));$skip(104); 
  //var tmp41 = tutorial.ColumnX("a") //ColumnImpl3("xexe")
  //tmp.toString

  var bb = sth().toString;System.out.println("""bb  : java.lang.String = """ + $show(bb ));$skip(50); 
  
  var nick = new Person("Nick","The Greek",12);System.out.println("""nick  : tutorial.Person = """ + $show(nick ))}
}

case class sth {

  override def toString = "nothing"

}

class Person(val firstName:String, val lastName:String, val age:Int)
{
  override def toString = "[Person: firstName="+firstName+" lastName="+lastName+
                          " age="+age+"]"

}
 
case class ColumnImpl2(value: String) {
  override def toString = "blablabla"//"tutorial.ColumnImpl(\"" + value + "\")"
}


/*
trait ColumnValue { def value: String }

trait RecordValue { def columns: List[ColumnValue] }

abstract trait Statement extends Positional
case class ColumnImpl(value: String) extends Statement with ColumnValue
case class RecordImpl(columns: List[ColumnImpl]) extends Statement with RecordValue

object CSVlazy {

  object CSVlazy extends RegexParsers {
    override val skipWhitespace = false // meaningful spaces in CSV

    def COMMA = ","
    def DQUOTE = "\""
    def DQUOTE2 = "\"\"" ^^ { case _ => "\"" } // combine 2 dquotes into 1
    def CRLF = "\r\n" | "\n"
    def TXT = "[^\",\r\n]".r
    def SPACES = "[ \t]+".r

    def file: Parser[List[RecordImpl]] = repsep(record, CRLF) <~ (CRLF?)

    def record: Parser[RecordImpl] =
      positioned(repsep(field, COMMA) ^^ { case f => RecordImpl(f) })

    def field: Parser[ColumnImpl] = escaped | nonescaped

    def escaped: Parser[ColumnImpl] = {
      ((SPACES?) ~> DQUOTE ~> ((TXT | COMMA | CRLF | DQUOTE2)*) <~ DQUOTE <~ (SPACES?)) ^^ {
        case ls => ColumnImpl(ls.mkString(""))
      }
    }

    def nonescaped: Parser[ColumnImpl] = (TXT*) ^^ { case ls => ColumnImpl(ls.mkString("")) }

    def parse(s: String) = parseAll(file, s) match {
      case Success(res, _) => res
      case e => throw new Exception(e.toString)
    }

    def nextRecordImpl(s: Iterator[String]): Stream[RecordImpl] = s.hasNext match {
      case false => Stream.empty
      case true => parse(record, s.next) match {
        case Success(res, _) => {
          //println("RECORD Read: " + res)
          res #:: nextRecordImpl(s)
        }
        case e => throw new Exception(e.toString)
      }
    }
  }

  val filePath = "/home/manolee/nodb/data/tmp.csv"
  val reader = new FileReader(filePath)
  val source = scala.io.Source.fromFile(filePath)

  val lines = source.getLines

  //source.close()
  //var csv = CSVlazy.nextRecordImpl(lines)

  println("break")
  //csv.toList
  var result = for {
    lineIter <- CSVlazy.nextRecordImpl(lines)
    //x = lineIter
    if lineIter.columns(0).value.toInt > 5
  } yield lineIter
  result.toList
  //CSVlazy.nextRecordImpl(lines) filter (x => x.columns(0).value > "8") toList

  Stream(RecordImpl(List(ColumnImpl("11"), ColumnImpl("10"), ColumnImpl("1342177281"), ColumnImpl("1"))))

}*/