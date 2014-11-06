package geotrellis.spark

import scala.collection.mutable

abstract sealed class KeyBound[K]

case class MinKeyBound[K]() extends KeyBound[K]
case class MaxKeyBound[K]() extends KeyBound[K]
case class ValueKeyBound[K](key: K) extends KeyBound[K]

trait KeyFilter[K] {
  def includeKey(key: K): Boolean
  def includePartition(minKey: KeyBound[K], maxKey: KeyBound[K]): Boolean
}

class FilterSet[K: Ordering] extends KeyFilter[K] {
  private var _filters = mutable.ListBuffer[KeyFilter[K]]()

  def withFilter(filter: KeyFilter[K]) = {
    _filters += filter
    this
  }

  def filters: Seq[KeyFilter[K]] = _filters

  def isEmpty = _filters.isEmpty

  def includeKey(key: K): Boolean =
    _filters.map(_.includeKey(key)).foldLeft(true)(_ && _)

  def includePartition(minKey: KeyBound[K], maxKey: KeyBound[K]): Boolean =
    _filters.map(_.includePartition(minKey, maxKey)).foldLeft(true)(_ && _)
}

object FilterSet {
  implicit def filtersToFilterSet[K: Ordering](filters: Seq[KeyFilter[K]]): FilterSet[K] =
    apply(filters)

  def EMPTY[K: Ordering] = new FilterSet[K]

  def apply[K: Ordering](): FilterSet[K] = new FilterSet[K]

  def apply[K: Ordering](filters: KeyFilter[K]*): FilterSet[K] =
    apply(filters)

  def apply[K: Ordering](filters: Seq[KeyFilter[K]])(implicit d: DummyImplicit): FilterSet[K] = {
    val fs = new FilterSet[K]
    filters.foreach(fs.withFilter(_))
    fs
  }
}
