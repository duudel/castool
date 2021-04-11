package castool.rql

import zio._
import zio.stream._

object Execution {
  type EnvService = Has[Env]
  type Stream = ZStream[EnvService, Throwable, InputRow]

  trait Env {
    def table(name: Name): Option[Stream]
  }

  def live(env: Env): ZLayer[Any, Nothing, zio.Has[Env]] = ZLayer.fromEffect {
    ZIO.succeed(env)
  }

  def run(q: Compiled.Source): Stream = {
    q match {
      case Compiled.Table(name) => tableStream(name)
      case Compiled.Cont(source, op) =>
        transform(op, run(source))
    }
  }

  def tableStream(name: Name): Stream = ZStream.accessStream[EnvService] { service =>
    val env = service.get
    env.table(name) match {
      case Some(stream) => stream
      case None =>
        val err = "Runtime: No such table as'" + name.n + "'"
        ZStream.fail(
          new NoSuchElementException()
        )
    }
  }

  def transform(op: Compiled.TopLevelOp, source: Stream): Stream = {
    op match {
      case Compiled.Where(expr) =>
        source.filter((expr.eval _).andThen(_.v))
      case Compiled.Project(names) =>
        source.map { input =>
          val values = names.map(name => name -> input.values(name))
          InputRow(values)
        }
      case Compiled.Extend(name, expr) =>
        source.map { input =>
          val value = expr.eval(input)
          val newValue = (name, value)
          InputRow(input.values + newValue)
        }
      case Compiled.OrderBy(names, order) =>
        def orderBy: InputRow => OrderValues = { input =>
          val values = names.map(name => input.values(name))
          OrderValues(values)
        }
        val folded = source.fold(Vector.empty[InputRow]) { case (acc, input) => acc :+ input }
        val it = folded.map(_.sortBy(orderBy)(if (order == Order.Asc) ordering else ordering.reverse))
        ZStream.fromIterableM(it)
      case Compiled.Summarize(aggregations, groupBy) =>
        val initialValues = aggregations.map(_.initialValue)
        if (groupBy.isEmpty) {
          val result = source
            .fold((initialValues, 0)) { case ((values, n), input) =>
              val results = values.zip(aggregations)
                .map { case (acc, aggregation) => aggregation.aggr(acc, input)
              }
              (results, n + 1)
            }.map { case (values, n) =>
              val results = aggregations
                .zip(values)
                .map { case (aggregation, value) =>
                  val finalized = aggregation.finalPhase(value, Num(n))
                    (aggregation.name, finalized)
                }
              InputRow(results)
            }
          ZStream.fromEffect(result)
        } else {
          val keyFunc = (input: InputRow) => groupBy.map(input.values)
          source.groupByKey(keyFunc).apply { case (key, stream) =>
            val result = stream
              .fold((initialValues, 0)) { case ((values, n), input) =>
                val results = values
                  .zip(aggregations)
                  .map { case (acc, aggregation) => aggregation.aggr(acc, input) }
                (results, n + 1)
              }
              .map { case (values, n) =>
                val results = aggregations
                  .zip(values)
                  .map { case (aggregation, value) =>
                    val finalized = aggregation.finalPhase(value, Num(n))
                    (aggregation.name, finalized)
                  }
                InputRow(results)
              }
            ZStream.fromEffect(result)
          }
        }
    }
  }

  private case class OrderValues(xs: Seq[Value])
  private implicit val ordering: Ordering[OrderValues] = new Ordering[OrderValues] {
    override def compare(as: OrderValues, bs: OrderValues): Int = {
      as.xs.zip(bs.xs).foldLeft(0) { case (acc, (a, b)) =>
        if (acc == 0) {
          (a, b) match {
            case (Null, Null) => 0
            case (Null, _) => -1
            case (_, Null) => 1
            case (Bool(true), Bool(true)) => 0
            case (Bool(false), Bool(true)) => -1
            case (Bool(true), Bool(false)) => 1
            case (Bool(false), Bool(false)) => 0
            case (Num(x), Num(y)) => if (x == y) 0 else if (x < y) -1 else 1
            case (Str(x), Str(y)) => x.compare(y)
            case (Obj(x), Obj(y)) => -1 // TODO: figure out how to compare or treat as an error in sem check
            case x => throw new MatchError(x)
          }
        } else {
          acc
        }
      }
    }
  }

}
