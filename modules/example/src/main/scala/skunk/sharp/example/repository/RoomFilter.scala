package skunk.sharp.example.repository

import cats.data.NonEmptyList
import skunk.sharp.contrib.ltree.LTree

import java.util.UUID

/**
 * Domain-level filter ADT for room listing. Each case captures one runtime predicate the API can ask for; the
 * repository materialises a `List[RoomFilter]` into a single `WHERE` clause by translating each case to a `Where[Void]`
 * (values baked via `Param.bind`) and AND-folding the result. The translation lives in [[RoomRepository]] alongside the
 * columns view it needs.
 *
 * Why a sealed ADT and not just `case class RoomQuery(minCapacity: Option[Int], …)`:
 *
 *   - Each case stays self-contained — adding a new filter is one new `case class` and one extra arm in the
 *     repository's `toWhere` matcher, no fiddling with optional fields scattered across layers.
 *   - The exhaustiveness check on the `match` keeps the repository honest when the ADT grows.
 *   - Multi-value cases (`NamesIn`, `IdsIn`) are first-class — they translate to `IN (…)` (OR semantics) inside a
 *     single AND-combined clause, which is closer to the intent than parallel optional fields would be.
 */
sealed trait RoomFilter

object RoomFilter {

  /** `capacity >= n` */
  final case class CapacityAtLeast(n: Int) extends RoomFilter

  /** `capacity <= n` */
  final case class CapacityAtMost(n: Int) extends RoomFilter

  /** `name ILIKE '%substring%'` — case-insensitive substring match. */
  final case class NameContains(substring: String) extends RoomFilter

  /** `name IN (…)` — OR-semantic exact-name match across a non-empty set. */
  final case class NamesIn(values: NonEmptyList[String]) extends RoomFilter

  /** `id IN (…)` — restrict to a non-empty set of ids. */
  final case class IdsIn(values: NonEmptyList[UUID]) extends RoomFilter

  /**
   * `location <@ prefix` — every room at or under the given ltree path. `prefix = "acme.dublin"` matches
   * `acme.dublin.floor3.r1` but not `acme.cork.floor1.r2`. Index-backed by the GiST index on `rooms.location`.
   */
  final case class LocationUnder(prefix: LTree) extends RoomFilter

  /** `amenities ? key` — rooms whose amenities map carries the given key (regardless of value). */
  final case class HasAmenity(key: String) extends RoomFilter

}
