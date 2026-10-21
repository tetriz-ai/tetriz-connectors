// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package cronstagger derives a stable, per-key minute/hour phase inside an operator's
// configured cron interval, so a fleet that shares one wall-clock schedule spreads its
// work across the period instead of firing together on :00 and :30.
//
// The stagger and the operator's expression pull against each other, and honouring only
// one of them is a real defect either way: an entry derived from the key hash alone
// discards the operator's cadence, and an entry copied verbatim starts the whole fleet
// together. The resolution is that the configured expression fixes the repeat and the
// per-key hash chooses the phase inside it, so every mode fires as often as the operator
// asked — only the offset within the period moves.
//
// The staggerKey is whatever the caller wants the phase to be stable per: an org ID for a
// per-org sweep, an org/connector key for a host job, an org/connector/operation key for a
// connector op. It is shared by the connector source and any host schedule source.
package cronstagger

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// minutesPerHour and hoursPerDay are the ranges the minute and hour cron fields span. A step
// is phase-shifted inside its own range, and a step that does not divide that range evenly
// costs the shifted key a firing, so the range length is arithmetic here rather than decoration.
const (
	minutesPerHour = 60
	hoursPerDay    = 24
)

// cronParser is the 5-field grammar the staggerable expressions are read with. Running the
// operator's expression through a real parser rather than a hand-rolled field check is what
// turns a schedule this package cannot honour into a warning instead of a silent fallback.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// runtimeCronParser mirrors the grammar asynq's PeriodicTaskManager actually runs entries with
// — the 5-field form PLUS the `@`-descriptor shorthand (`@hourly`, `@every 30m`). It exists only
// to tell "the runtime can honour this, we just cannot phase-shift it" (emit verbatim) apart
// from "no scheduler can parse this" (fall back to a per-key daily slot). A descriptor has no
// 5-field form to offset, so it is honoured as written rather than silently collapsed to daily.
var runtimeCronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// mode names which field of the configured expression carries the per-key offset.
type mode int

const (
	// modeDaily is the fallback for an expression there is nothing to honour in: unset, or one
	// the cron parser rejects. The hash chooses hour and minute across the whole day.
	modeDaily mode = iota
	// modeMinutePhase is a sub-hourly repeat such as `*/20`. The key fires every step minutes,
	// offset into its own phase of that period; the hour field — and so any window the operator
	// restricted the repeat to — is left exactly as configured.
	modeMinutePhase
	// modeHourPhase is a multi-hour repeat such as `0 */6`. The hash chooses both which phase
	// of the step-hour period the key lands in and the minute inside that hour.
	modeHourPhase
	// modeMinuteOnly is an hour selection that stands as written — a literal, a list, a range,
	// or `*` — paired with a minute field naming one minute. It fires once per selected hour
	// whichever minute that is, so the minute is the stagger's to move and moving it leaves the
	// cadence alone.
	modeMinuteOnly
	// modeNone is an expression honoured verbatim because no field can carry a per-key offset
	// without changing how often it fires. Every key fires together.
	modeNone
)

// Slot is the operator's configured schedule, resolved once per fetch into the single
// decision EntryFor needs: which field of the expression the per-key stagger owns.
//
// The zero value is a valid modeDaily slot, but a caller that resolved nothing (an empty base
// expression it means to skip) should not call EntryFor at all rather than emit a daily entry.
type Slot struct {
	mode mode
	// fields is the configured expression's fields, whose day-of-month, month and day-of-week
	// entries always pass through untouched. Nil for modeDaily, which carries nothing. For
	// modeNone it is honoured verbatim, so it may be a 5-field cron or an `@`-descriptor's tokens.
	fields []string
	// step is the length of the repeat period the hash phase-shifts inside — minutes for
	// modeMinutePhase, hours for modeHourPhase. Zero for the other modes.
	step int
}

// Resolve parses the configured schedule ONCE for the whole fetch rather than per key, and
// says out loud anything it could not honour. sourceName names the calling schedule source in
// any warning it emits.
//
// Silence is the failure mode this function exists to prevent. An expression the code quietly
// replaces with a fallback means the operator set a cadence, the fleet ignored it, and nothing
// anywhere says so — which is how a config surface comes to promise a control that is not
// connected to anything. So every departure from the configured expression warns, once per fetch
// rather than once per key, keeping a static misconfiguration visible without turning it into
// log volume proportional to the fleet.
func Resolve(ctx context.Context, sourceName, configured string) Slot {
	if configured == "" {
		// An unset schedule is not a discarded one: nothing was configured for the fleet to
		// ignore, and the per-key daily slot is the honest answer to it.
		return Slot{mode: modeDaily}
	}
	fields := strings.Fields(configured)
	if len(fields) != 5 {
		// Not a 5-field expression. A genuinely unparseable string falls back to the per-key
		// daily slot (and reports why). But if the scheduler that runs these entries can still
		// parse it — an `@`-descriptor like `@hourly` / `@every 30m` — honour it verbatim rather
		// than collapsing its cadence to daily; a descriptor has no field to phase-shift, so the
		// fleet fires together but keeps the operator's cadence.
		if _, parseErr := runtimeCronParser.Parse(configured); parseErr != nil {
			warnScheduleNotHonoured(ctx, sourceName, configured, parseErr.Error())
			return Slot{mode: modeDaily}
		}
		warnScheduleUnstaggerable(ctx, sourceName, configured, "not a 5-field cron expression; cannot be per-key staggered")
		return Slot{mode: modeNone, fields: fields}
	}
	if _, err := cronParser.Parse(configured); err != nil {
		warnScheduleNotHonoured(ctx, sourceName, configured, err.Error())
		return Slot{mode: modeDaily}
	}

	if step, ok := parseCronStep(fields[0], minutesPerHour-1); ok {
		warnUnevenStep(ctx, sourceName, configured, "minute", minutesPerHour, step)
		return Slot{mode: modeMinutePhase, fields: fields, step: step}
	}
	if !isSingleCronValue(fields[0], minutesPerHour-1) {
		// A minute field naming several minutes (`*`, `0,30`, `5-50/9`) fires more than once an
		// hour by a count only that field knows, so replacing it would change the cadence and
		// phase-shifting it is undefined. The expression stands; the stagger is what is lost.
		warnScheduleUnstaggerable(ctx, sourceName, configured, "minute field selects several minutes per hour")
		return Slot{mode: modeNone, fields: fields}
	}
	if step, ok := parseCronStep(fields[1], hoursPerDay-1); ok {
		warnUnevenStep(ctx, sourceName, configured, "hour", hoursPerDay, step)
		return Slot{mode: modeHourPhase, fields: fields, step: step}
	}
	return Slot{mode: modeMinuteOnly, fields: fields}
}

// warnScheduleNotHonoured reports a configured expression that reached no entry at all: the fleet
// fell back to the per-key daily slot, so the operator's cadence is in effect nowhere.
func warnScheduleNotHonoured(ctx context.Context, sourceName, configured, reason string) {
	logger.WarnWithCtx(ctx, sourceName+": configured schedule was NOT honoured; every key fell back to a per-key daily slot",
		zap.String("source", sourceName),
		zap.String("configured", configured),
		zap.String("used", "<hash-minute> <hash-hour> * * *"),
		zap.String("reason", reason))
}

// warnScheduleUnstaggerable notes, at DEBUG, an expression that IS honoured, verbatim, but
// carries no per-key offset — so the whole fleet fires at the same instant. The schedule is
// valid and some defaults are written this way, so a healthy start does not report it.
func warnScheduleUnstaggerable(ctx context.Context, sourceName, configured, reason string) {
	logger.Debug(ctx, sourceName+": configured schedule is honoured verbatim but cannot be per-key staggered; the fleet fires together",
		zap.String("source", sourceName),
		zap.String("configured", configured),
		zap.String("used", configured),
		zap.String("reason", reason))
}

// warnUnevenStep reports a step that does not divide its field's range evenly, the one way a
// phase-shifted entry still differs from the configured one: the cadence is kept, but the last
// interval before the range restarts is shorter than the step, so a shifted key can fire once
// less per period than the expression names.
func warnUnevenStep(ctx context.Context, sourceName, configured, unit string, period, step int) {
	if period%step == 0 {
		return
	}
	logger.WarnWithCtx(ctx, sourceName+": configured step does not divide its period evenly; a staggered key may fire once less per period than the expression names",
		zap.String("source", sourceName),
		zap.String("configured", configured),
		zap.String("unit", unit),
		zap.Int("step", step),
		zap.Int("period", period))
}

// parseCronStep reports the step of a `*/n` field, and false for every other form.
//
// A step of 1 is reported as no step: `*/1` selects the whole range, so there is no period to
// offset inside. A step past the field's upper bound is no step either — it selects a single
// value, which the caller's remaining modes already place correctly.
func parseCronStep(field string, upper int) (int, bool) {
	rest, ok := strings.CutPrefix(field, "*/")
	if !ok {
		return 0, false
	}
	step, ok := parseBoundedUint(rest, upper)
	if !ok || step < 2 {
		return 0, false
	}
	return step, true
}

// isSingleCronValue reports whether a field names exactly one value in [0,upper].
//
// Parsed by hand rather than through strconv so there is no error to discard: a `*` or a `2,14`
// is a correct expression this function simply has no single answer for, and dressing that up as
// a parse error would be a failure the caller must then decide to ignore.
func isSingleCronValue(field string, upper int) bool {
	_, ok := parseBoundedUint(field, upper)
	return ok
}

// parseBoundedUint reads an all-digit field as an int in [0,upper], and false if it is empty,
// non-numeric, or out of range. Parsed by hand rather than through strconv so the range check is
// part of the parse and there is no error value to discard; shared by the single-value, step, and
// range-bound parsers so none of them hand-rolls the digit walk.
func parseBoundedUint(s string, upper int) (int, bool) {
	if !isAllDigits(s) {
		return 0, false
	}
	value := 0
	for _, r := range s {
		value = value*10 + int(r-'0')
		if value > upper {
			return 0, false
		}
	}
	return value, true
}

func isAllDigits(field string) bool {
	if field == "" {
		return false
	}
	for _, r := range field {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// EntryFor places one key inside the repeat the operator configured, rather than across the day.
//
// The key hash supplies the phase and the configured expression supplies everything else, so two
// keys on one schedule fire the same number of times and never at the same instant. Where the
// phase goes is what the mode decided; every field the mode does not name is the operator's and
// passes through as written.
func (c Slot) EntryFor(staggerKey string) string {
	// modeNone is emitted verbatim (a 5-field cron or an `@`-descriptor's tokens), so it is
	// exempt from the 5-field guard the phase modes need — a descriptor has fewer than 5 fields.
	if c.mode != modeDaily && c.mode != modeNone && len(c.fields) != 5 {
		return DailyStaggeredCron(staggerKey)
	}
	sum := hashKey(staggerKey)
	switch c.mode {
	case modeMinutePhase:
		// `p-59/n` fires at p, p+n, p+2n … inside every hour the operator selected: the same
		// n-minute cadence as `*/n`, moved onto this key's phase. p < n is what keeps it so.
		return fmt.Sprintf("%d-%d/%d %s %s %s %s",
			sum%uint32(c.step), minutesPerHour-1, c.step,
			c.fields[1], c.fields[2], c.fields[3], c.fields[4])
	case modeHourPhase:
		// Split mixed-radix (low digits → hour phase, next → minute) so the two stay decorrelated
		// and keys fan out across the period instead of sharing one minute inside it.
		return fmt.Sprintf("%d %d-%d/%d %s %s %s",
			(sum/uint32(c.step))%minutesPerHour, sum%uint32(c.step), hoursPerDay-1, c.step,
			c.fields[2], c.fields[3], c.fields[4])
	case modeMinuteOnly:
		return fmt.Sprintf("%d %s %s %s %s",
			sum%minutesPerHour, c.fields[1], c.fields[2], c.fields[3], c.fields[4])
	case modeNone:
		return strings.Join(c.fields, " ")
	default:
		return DailyStaggeredCron(staggerKey)
	}
}

// GroupStaggerMode is the Slot.Mode-style label for a schedule emitted by GroupEntryFor
// rather than a per-key Slot. A stagger-group member never goes through Resolve, so it has
// no Slot.Mode; this names the group path for the cron-entry metric, alongside the five
// Slot.Mode strings.
const GroupStaggerMode = "group"

// GroupEntryFor places one member of a stagger group inside the phase the whole group rotates
// on together, and returns its minute-phase cron entry `<minute>-59/step * * * *`.
//
// Every member resolves the SAME base phase φ = hash(groupKey) % step from the shared groupKey
// (an org+connector+group key, NOT a per-operation one), then sits at (φ + offsetMinutes) mod
// step. Two properties follow, and they are the whole reason a group primitive exists where the
// per-key EntryFor would not do: offsets that are distinct mod step stay distinct after adding
// any φ, so the members of one group never share a firing minute — the spacing a shared per-org
// rate-limit bucket needs — while a different groupKey (a different org) yields a different φ, so
// the group shifts as a unit and the fleet does not fire in lockstep.
//
// step is the repeat period in minutes; all members of a group must agree on it (derive it with
// GroupStaggerStep). offsetMinutes must be non-negative and distinct mod step across the group —
// two members sharing a residue collapse onto one minute, which is the collision the primitive is
// built to prevent. A step outside [1,59] has no in-hour period to place a member into (and a step
// past the hour would emit a range start the scheduler rejects), and a negative offset would wrap;
// each degrades to the per-key daily slot, so a misuse fails visibly-daily, not with a panic or an
// entry the scheduler drops.
func GroupEntryFor(groupKey string, step, offsetMinutes int) string {
	if step <= 0 || step >= minutesPerHour || offsetMinutes < 0 {
		return DailyStaggeredCron(groupKey)
	}
	phase := hashKey(groupKey) % uint32(step)
	minute := (phase + uint32(offsetMinutes)) % uint32(step)
	return fmt.Sprintf("%d-%d/%d * * * *", minute, minutesPerHour-1, step)
}

// GroupStaggerStep reports the repeat step (minutes) of a schedule a stagger group can place its
// members inside, and false for any other shape. Because GroupEntryFor emits `<minute>-59/step *
// * * *`, an eligible schedule is a 5-field cron whose minute field spans the full hour as a
// `*/step` (or a phase-shifted `p-59/step`) step and whose hour, day-of-month, month and
// day-of-week fields are all `*`. A schedule that restricted any of those other fields is
// rejected rather than silently widened by the group emit.
func GroupStaggerStep(configured string) (int, bool) {
	fields := strings.Fields(configured)
	if len(fields) != 5 {
		return 0, false
	}
	for _, f := range fields[1:] {
		if f != "*" {
			return 0, false
		}
	}
	return minutePhaseStep(fields[0])
}

// minutePhaseStep reads the repeat step off a minute field a group can place its members inside,
// and false for any other shape. Eligibility is stricter than the per-op modeMinutePhase because
// GroupEntryFor re-emits EVERY member as `<phase>-59/step` with phase in [0,step), and that must
// fire the configured number of times an hour for every phase and every member:
//   - the field is a full-hour step — `*/step`, or a phase-shifted `p-59/step` whose start is
//     within the first period (p < step) and whose range ends at the last minute (:59); a range
//     starting at/after the step or stopping short fires fewer times an hour than `*/step`.
//   - step divides the hour evenly (60 % step == 0); an uneven step fires once less for a phase
//     in the short tail, so two members — or one org versus another — would run at different
//     cadences, defeating the equal-spacing the group guarantees.
func minutePhaseStep(field string) (int, bool) {
	step, ok := fullHourStep(field)
	if !ok || minutesPerHour%step != 0 {
		return 0, false
	}
	return step, true
}

// fullHourStep reads the repeat step off a minute field that spans the whole hour: `*/step`, or a
// phase-shifted `p-59/step` whose start is within the first period (p < step) and whose range ends
// at the last minute. A range starting at/after the step or ending short of :59 fires fewer times
// an hour than `*/step`, so it returns false rather than a step it cannot honour uniformly. The
// even-divisor check is the caller's (minutePhaseStep), not this shape parser's.
func fullHourStep(field string) (int, bool) {
	if step, ok := parseCronStep(field, minutesPerHour-1); ok {
		return step, true
	}
	base, stepStr, hasStep := strings.Cut(field, "/")
	if !hasStep {
		return 0, false
	}
	lo, hi, hasRange := strings.Cut(base, "-")
	if !hasRange {
		return 0, false
	}
	loVal, loOK := parseBoundedUint(lo, minutesPerHour-1)
	hiVal, hiOK := parseBoundedUint(hi, minutesPerHour-1)
	step, stepOK := parseBoundedUint(stepStr, minutesPerHour-1)
	if !loOK || !hiOK || !stepOK || hiVal != minutesPerHour-1 || step < 2 || loVal >= step {
		return 0, false
	}
	return step, true
}

// Mode returns a stable snake_case name for how this slot's schedule is staggered — which
// field of the configured expression carries the per-key offset, or that none can. It is
// the label the scheduler's cron-entry metric buckets on, so the names are a contract with
// that metric's label, deliberately independent of the internal enum's order or
// spelling. Only "verbatim" (modeNone) is emitted un-staggered — every key fires together;
// "daily_fallback" is still per-key staggered (EntryFor hashes it via DailyStaggeredCron), it
// just means the configured cadence was unset or unparseable. The zero-value Slot reports
// daily_fallback, matching its modeDaily zero value.
func (c Slot) Mode() string {
	switch c.mode {
	case modeMinutePhase:
		return "minute_phase"
	case modeHourPhase:
		return "hour_phase"
	case modeMinuteOnly:
		return "minute_only"
	case modeNone:
		return "verbatim"
	default: // modeDaily and the zero value
		return "daily_fallback"
	}
}

// DailyStaggeredCron returns a 5-field cron that fires once a day at a per-key minute and hour
// derived from a stable hash of the staggerKey. Every key fires daily; the minute+hour stagger
// is what spreads a per-key full-window scan across the day instead of all keys stampeding one
// slot.
func DailyStaggeredCron(staggerKey string) string {
	minute, hour := minuteHourPhase(staggerKey)
	return fmt.Sprintf("%d %d * * *", minute, hour)
}

// MonthlyStaggeredCron returns a 5-field cron that fires once a month on the 1st at a per-key
// minute and hour derived from a stable hash of the staggerKey. Every key shares day-of-month 1;
// the minute+hour stagger is what spreads derivation load across that day instead of all keys
// stampeding midnight.
func MonthlyStaggeredCron(staggerKey string) string {
	minute, hour := minuteHourPhase(staggerKey)
	return fmt.Sprintf("%d %d 1 * *", minute, hour)
}

// minuteHourPhase derives a per-key within-day (minute, hour) slot from the stable key hash. The
// hash is split mixed-radix (low digits → hour in [0,23], next → minute in [0,59]) so the two
// fields stay decorrelated and keys fan out across all 1440 within-day slots rather than sharing
// one minute inside an hour.
func minuteHourPhase(staggerKey string) (minute, hour int) {
	sum := hashKey(staggerKey)
	return int((sum / hoursPerDay) % minutesPerHour), int(sum % hoursPerDay)
}

// hashKey is the stable FNV-32a hash the per-key phase is derived from. Stable across processes
// and deterministic for a given key, so an entry does not move between scheduler ticks or replicas.
func hashKey(staggerKey string) uint32 {
	h := fnv.New32a()
	// Write never errors for hash.Hash32; ignore the return to keep the call clean.
	_, _ = h.Write([]byte(staggerKey))
	return h.Sum32()
}
