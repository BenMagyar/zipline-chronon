package ai.chronon.aggregator.base

import java.util
import java.util.Comparator

object UniqueOrderByLimit {

  def initState[T, OrderType]: State[T, OrderType] =
    State(new util.ArrayList[T](), new util.HashSet[Long](), null.asInstanceOf[OrderType])

  case class State[T, OrderType](elems: java.util.ArrayList[T],
                                 ids: java.util.HashSet[Long],
                                 var orderWaterMark: OrderType)

  // UniqueTopKAggregator will create this Operator from input args and call the public methods in its implementation
  case class Operator[T, OrderType: Ordering](getOrderKey: T => OrderType,
                                              getId: T => Long,
                                              k: Int,
                                              maxSize: Int,
                                              topK: Boolean = true) {

    private val ordering = implicitly[Ordering[OrderType]]

    // to be used in "finalize" of the aggregator
    def sortAndPrune(state: State[T, OrderType]): Unit = {

      sort(state)
      val elems = state.elems

      while (elems.size > k) {
        val removed = elems.remove(elems.size - 1)
        state.ids.remove(getId(removed))
      }

      if (elems.size > 0) {
        state.orderWaterMark = getOrderKey(elems.get(elems.size - 1))
      }
    }

    // to be used to impl "denormalize" of the aggregator
    def buildStateFromElems(elems: java.util.ArrayList[T]): State[T, OrderType] = {

      val state: State[T, OrderType] = UniqueOrderByLimit.initState[T, OrderType]
      val it = elems.iterator()

      while (it.hasNext) {
        insert(it.next(), state)
      }

      state
    }

    def insert(elem: T, state: State[T, OrderType]): Unit = {

      val id = getId(elem)

      if (state.ids.contains(id)) {
        var index = 0
        while (index < state.elems.size() && getId(state.elems.get(index)) != id) {
          index += 1
        }

        if (index == state.elems.size()) {
          throw new IllegalStateException(s"UniqueTopK state contains id $id without its corresponding element")
        }

        val existing = state.elems.get(index)
        val comparison = ordering.compare(getOrderKey(elem), getOrderKey(existing))
        if ((topK && comparison > 0) || (!topK && comparison < 0)) {
          state.elems.set(index, elem)
        } else if (
          comparison == 0 &&
          !util.Objects.deepEquals(existing.asInstanceOf[AnyRef], elem.asInstanceOf[AnyRef])
        ) {
          throw new IllegalArgumentException(
            s"Conflicting UNIQUE_TOP_K rows for unique_id=$id, sort_key=${getOrderKey(existing)}")
        }
        return
      }

      val orderKey = getOrderKey(elem)

      if (topK) {
        if (state.elems.size() < k) {

          state.orderWaterMark = if (state.orderWaterMark == null || ordering.lt(orderKey, state.orderWaterMark)) {
            orderKey
          } else {
            state.orderWaterMark
          }

          state.elems.add(elem)
          state.ids.add(id)

        } else if (ordering.gteq(orderKey, state.orderWaterMark)) {

          state.elems.add(elem)
          state.ids.add(id)

        }
      } else {
        if (state.elems.size() < k) {
          // keep min
          state.orderWaterMark = if (state.orderWaterMark == null || ordering.gt(orderKey, state.orderWaterMark)) {
            orderKey
          } else {
            state.orderWaterMark
          }

          state.elems.add(elem)
          state.ids.add(id)

        } else if (ordering.lteq(orderKey, state.orderWaterMark)) {

          state.elems.add(elem)
          state.ids.add(id)

        }
      }

      if (state.elems.size() > maxSize) {
        sortAndPrune(state)
      }
    }

    private def sort(state: State[T, OrderType]): Unit = {

      state.elems.sort(new Comparator[T] {
        override def compare(o1: T, o2: T): Int = {
          val o1Key = getOrderKey(o1)
          val o2Key = getOrderKey(o2)

          val orderComparison =
            if (topK) ordering.compare(o2Key, o1Key)
            else ordering.compare(o1Key, o2Key)

          if (orderComparison != 0) orderComparison else java.lang.Long.compare(getId(o1), getId(o2))
        }
      })
    }
  }

}
