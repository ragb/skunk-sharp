package skunk.sharp.example.repository

import cats.data.NonEmptyList

import java.time.LocalDate
import java.util.UUID

/**
 * Domain-level filter ADT for booking listing. Mirrors [[RoomFilter]] — see that file for the rationale on
 * sealed ADTs versus a wide optional-fields payload. The translation to `Where[Void]` lives in
 * [[BookingRepository]] alongside the columns view it needs.
 *
 * Date filters operate on the `period: PgRange[LocalDate]` column via the range operators (`@>`, `&&`, lower /
 * upper accessors). The repository handles the SQL details; this layer just names the predicate.
 */
sealed trait BookingFilter

object BookingFilter {

  /** `room_id IN (…)` — restrict to bookings of a non-empty set of rooms. */
  final case class RoomsIn(roomIds: NonEmptyList[UUID]) extends BookingFilter

  /** `booker_name ILIKE '%substring%'`. */
  final case class BookerNameContains(substring: String) extends BookingFilter

  /** `title ILIKE '%substring%'`. */
  final case class TitleContains(substring: String) extends BookingFilter

  /** `period && [from, to)` — booking's period overlaps the requested window. */
  final case class OverlapsPeriod(from: LocalDate, to: LocalDate) extends BookingFilter

  /** `lower(period) >= date` — bookings starting on or after the date. */
  final case class StartsOnOrAfter(date: LocalDate) extends BookingFilter

  /** `upper(period) <= date` — bookings ending on or before the date. */
  final case class EndsOnOrBefore(date: LocalDate) extends BookingFilter

}
