---
layout: operations
title: Creating your own

tutorial: operations
num: 5
---

Ideally, a new geoprocessing service can be composed with existing operations
that have already been created for you.  The tutorial, for example, shows a
series of examples of a service being created by composing existing operations.

But sometimes it is necessary to create new operations instead of simply
composing a new process with existing operations. There is a short method and
the full method creating new operations.

When thinking about operations, think about them like a mathematical function--
operations take input and produce an output.  The inputs to all operations are 
operations themselves.  You can think about this as a data pipeline, in which
each operation's output is connected to the input of its parent operations.

#### Short method 

Let's create a simple operation that adds two integers together.

We'll make an operation that can be instantiated with the following code:


    val a = AddInts(1, 2)
    val result = server.run(a)

This is an example of using the short method to define an operation:

    case class AddInts(a:Op[Int], b:Op[Int]) 
        extends Op2(a, b) ({ (a, b) => 
    
        // Add the ints
        a + b
    })

The operation above is called ``AddInts``.  ``Op`` is short for Operation.
Right after AddInts are the inputs to this operation, called ``a`` and ``b``.
The declaration ``a:Op[Int]`` means that the first input is of the type ``Op[Int]``.
``[Int]`` is the type parameter of ``Op``, and it means that a is an operation
that outputs an Int (an integer). The class ``AddInts`` exends the class
``Op2``, which means that it is an operation that takes two inputs. If this
operation had only 1 input, we'd use ``Op1`` instead of ``Op2``. 

For example:


    case class AddOne(a:Op[Int]) extends Op1(a) ({ a => 
        a + 1
    })

The syntax in the middle, e.g.:


    { (a,b) => 
      a + b
    }

means that we are making a function that takes two arguments (a and b) and we
define the body of the function that does the actual work with the inputs
between the two braces.

You can see lots of examples of Operations in `src/main/geotrellis/op`.

#### Full method

Instead of the short syntax (in which operations extend `Op1`..`OpN`) you can
create a class which extends `Op[T]`. This approach exposes the underlying
methods that operations use to load data (via the `Context` object) and also to
stage calculations across multipe steps.

    case class Add(a:Op[Int], b:Op[Int]) extends Op[Int] {
      def _run(context:Context) = {
        runAsync(List(a, b))
      }
  
      def nextSteps:Steps = {
        case (a:Int) :: (b:Int) :: Nil => {
          a + b
        }
      }
    }

The _run method is where the execution of the operation begins. We call
`runAsync` with a list of operations to tell the server to asynchronous execute
our child operations, and then the partial function `nextSteps` is executed
when all of the results have been retrieved.

If we need to pass any information forward, we can include it in the List given
to `runAsync` All operations passed to `runAsync` will be executed (in
parallel), while other values will simply be passed through. The resulting list
will be matched against `nextSteps`, which will continue executing the
calculation. The syntax used in `nextSteps` (involving one of more `case`
statements) is the same as other pattern matching in Scala.

For example, the following code tries to find the pattern of a two-element list
where the first element and second elements are integers. If the input matches
this pattern, it saves the first element in ``a`` and the second in ``b`` and
executes the provide block of code.

If you want to understand more about the machinery at work here, see the
*Architecture Concepts* section and the Server code
(`geotrellis/process/server.scala` and `geotrellis/process/actors.scala`).
