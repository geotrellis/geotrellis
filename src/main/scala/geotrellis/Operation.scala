package geotrellis

import geotrellis.process._
import scala.{PartialFunction => PF}

import akka.actor._

import scala.language.implicitConversions

/**
 * Base Operation for all GeoTrellis functionality. All other operations must
 * extend this trait.
 */
abstract class Operation[+T] extends Product with Serializable {
  type Steps = PartialFunction[Any, StepOutput[T]]
  type Args = List[Any]
  val nextSteps:PartialFunction[Any,StepOutput[T]]

  val debug = false
  private def log(msg:String) = if(debug) println(msg)

  /**
    * Return operation identified (class simple name).
    */
  private var _name = getClass.getSimpleName
  def name: String = _name
  def withName(n:String):Operation[T] = { _name += s" ($n)"; this }

  protected[geotrellis] def _run(context:Context): StepOutput[T]
  
  /**
    * Execute this operation and return the result.  
    */
  def run(context:Context): StepOutput[T] =
    _run(context)

  def runAsync(args:Args): StepOutput[T] = {
    StepRequiresAsync[T](args, { (nextArgs:Args) =>
      processNextSteps(nextArgs)
    })
  }

  def processNextSteps(args:Args):StepOutput[T] = nextSteps(args)

  /** Returns an operation whose children will be executed on a remote cluster.
    * 
    *  This method will cause the operations executed as a result of this operation
    *  (input operations and any operations necessary for this computation) to be 
    *  dispatched to and executed on a remote cluster.
    *
    */
  def dispatch(cluster:ActorRef) =
    DispatchedOperation(this, cluster)

  /** Returns an operation that will be executed on a remote cluster.
    *
    *  This method will cause the specified operation to be executed on a remote
    *  cluster.
    *
    *  Note that if you wish to distribute the work of a single operation, you
    *  should probably be using dispatch() instead of remote().
    */
  def remote(cluster:ActorRef) = logic.Do1(this)(x => x).dispatch(cluster)
  

  /**
    * Create a new operation with a function that takes the result of this operation
    * and returns a new operation.
    */
  def flatMap[U](f:T=>Operation[U]):Operation[U] = new CompositeOperation(this,f)

  /**
    * Create a new operation that returns the result of the provided function that
    * takes this operation's result as its argument.
    */
  def map[U](f:(T)=>U):Operation[U] = logic.Do1(this)(f)

  /**
    * Create an operation that applies the function f to the result of this operation,
    * but returns nothing.
    */
  def foreach[U](f:(T)=>U):Unit = logic.Do1(this) { t:T =>
    f(t)
    ()
  }

  /**
    * Create a new operation with a function that takes the result of this operation
    * and returns a new operation.
    * 
    * Same as flatMap.
    */
  def withResult[U](f:T=>Operation[U]):Operation[U] = flatMap(f)


  //TODO: how should filter be implemented for list comprehensions?
  def filter(f:(T) => Boolean) = this

  def andThen[U](f:T => Op[U]) = flatMap(f)

  /** Call the given function with this operation as its argument.
    *
    * This is primarily useful for code readability.
    * @see http://debasishg.blogspot.com/2009/09/thrush-combinator-in-scala.html
    */
  def into[U] (f: (Operation[T]) => U):U = f(this)

  def prettyString:String = {
    val sb = new StringBuilder
    sb.append(s"$name(")
    val arity = this.productArity
    for(i <- 0 until arity) {
      this.productElement(i) match {
        case lit:Literal[_] =>
          sb.append(s"LT{${lit.value}}")
        case op:Operation[_] =>
          sb.append(s"OP{${op.name}}")
        case x => 
          sb.append(s"$x")
      }
      if(i < arity - 1) { sb.append(",") }
    }
    sb.append(")")
    sb.toString
  }
}


/**
 * Given an operation and a function that takes the result of that operation and returns
 * a new operation, return an operation of the return type of the function.
 * 
 * If the initial operation is g, you can think of this operation as f(g(x)) 
 */
case class CompositeOperation[+T,U](gOp:Op[U], f:(U) => Op[T]) extends Operation[T] {
  def _run(context:Context) = runAsync('firstOp :: gOp :: Nil)

  val nextSteps:Steps = {
    case 'firstOp :: u :: Nil => runAsync('result :: f(u.asInstanceOf[U]) :: Nil) 
    case 'result :: t :: Nil => Result(t.asInstanceOf[T])
  } 
}

case object UnboundOperation extends Op0[Nothing](throw new Exception("foo"))

abstract class OperationWrapper[+T](op:Op[T]) extends Operation[T] {
  def _run(context:Context) = op._run(context)
  val nextSteps:Steps = op.nextSteps
}

case class DispatchedOperation[+T](val op:Op[T], val dispatcher:ActorRef)
extends OperationWrapper(logic.Force(op)) {}

case class RemoteOperation[+T](val op:Op[T], cluster:ActorRef)
extends OperationWrapper(logic.Force(op)) {}

object Operation {
  implicit def implicitLiteralVal[A <: AnyVal](a:A)(implicit m:Manifest[A]):Operation[A] = Literal(a)
  implicit def implicitLiteralRef[A <: AnyRef](a:A):Operation[A] = Literal(a)
}

/**
 * Below are the Op0 - Op6 abstract classes.
 *
 * These are useful for easily defining operations which just want to evaluate
 * their child operations and then run a single function afterwards.
 *
 * For example:
 *
 * case class Add2(x:Op[Int], y:Op[Int]) extends Op2(x, y)(_ + _)
 */

abstract class Op0[T](f:()=>StepOutput[T]) extends Operation[T] {
  def _run(context:Context) = f()
  val nextSteps:Steps = {
    case _ => sys.error("should not be called")
  }
}

class Op1[A,T](a:Op[A])(f:(A)=>StepOutput[T]) extends Operation[T] {
  def _run(context:Context) = runAsync(List(a))

  def productArity = 1
  def canEqual(other:Any) = other.isInstanceOf[Op1[_,_]]
  def productElement(n:Int) = if (n == 0) a else throw new IndexOutOfBoundsException()
  val myNextSteps:PartialFunction[Any,StepOutput[T]] = {
    case a :: Nil => f(a.asInstanceOf[A])
  }
  val nextSteps = myNextSteps
}

class Op2[A,B,T](a:Op[A], b:Op[B]) (f:(A,B)=>StepOutput[T]) extends Operation[T] {
  def productArity = 2
  def canEqual(other:Any) = other.isInstanceOf[Op2[_,_,_]]
  def productElement(n:Int) = n match {
    case 0 => a
    case 1 => b
    case _ => throw new IndexOutOfBoundsException()
  }
  def _run(context:Context) = runAsync(List(a,b))
  val nextSteps:Steps = { 
    case a :: b :: Nil => f(a.asInstanceOf[A], b.asInstanceOf[B])
  }
}

class Op3[A,B,C,T](a:Op[A],b:Op[B],c:Op[C])
(f:(A,B,C)=>StepOutput[T]) extends Operation[T] {
  def productArity = 3
  def canEqual(other:Any) = other.isInstanceOf[Op3[_,_,_,_]]
  def productElement(n:Int) = n match {
    case 0 => a
    case 1 => b
    case 2 => c
    case _ => throw new IndexOutOfBoundsException()
  }
  def _run(context:Context) = runAsync(List(a,b,c))
  val nextSteps:Steps = { 
    case a :: b :: c :: Nil => {
      f(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C])
    }
  }
}

class Op4[A,B,C,D,T](a:Op[A],b:Op[B],c:Op[C],d:Op[D])
(f:(A,B,C,D)=>StepOutput[T]) extends Operation[T] {
  def productArity = 4
  def canEqual(other:Any) = other.isInstanceOf[Op4[_,_,_,_,_]]
  def productElement(n:Int) = n match {
    case 0 => a
    case 1 => b
    case 2 => c
    case 3 => d
    case _ => throw new IndexOutOfBoundsException()
  }
  def _run(context:Context) = runAsync(List(a,b,c,d))
  val nextSteps:Steps = { 
    case a :: b :: c :: d :: Nil => {
      f(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C], d.asInstanceOf[D])
    }
  }

}

abstract class Op5[A,B,C,D,E,T](a:Op[A],b:Op[B],c:Op[C],d:Op[D],e:Op[E])
(f:(A,B,C,D,E)=>StepOutput[T]) extends Operation[T] {
  def _run(context:Context) = runAsync(List(a,b,c,d,e))
  val nextSteps:Steps = {
    case a :: b :: c :: d :: e :: Nil => {
      f(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C],
        d.asInstanceOf[D], e.asInstanceOf[E])
    }
  }
  
}

abstract class Op6[A,B,C,D,E,F,T]
(a:Op[A],b:Op[B],c:Op[C],d:Op[D],e:Op[E],f:Op[F])
(ff:(A,B,C,D,E,F)=>StepOutput[T]) extends Operation[T] {
  def _run(context:Context) = runAsync(List(a,b,c,d,e,f))
  val nextSteps:Steps = {
    case a :: b :: c :: d :: e :: f :: Nil => {
      ff(a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C],
         d.asInstanceOf[D], e.asInstanceOf[E], f.asInstanceOf[F])
    }
  }
}
