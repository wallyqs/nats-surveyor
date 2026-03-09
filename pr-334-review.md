# PR #334 Review: Collect raftz from system account

## Summary

This PR adds three new Prometheus metrics for the meta Raft group by querying the
`SERVER.PING.RAFTZ` endpoint:
- `nats_core_raftz_meta_committed` - Highest committed log entry index
- `nats_core_raftz_meta_applied` - Highest applied log entry index
- `nats_core_raftz_meta_pindex` - Log entry index at last snapshot

It follows the existing pattern established by gatewayz collection and includes a
Grafana dashboard update with new panels.

## Overall Assessment

The implementation is clean and follows existing conventions well. The code
mirrors the `gatewayz` pattern closely, making it easy to review. There are a few
items worth discussing before merge.

---

## Issues to Address

### 1. Unrelated changes bundled in the PR

Several changes are unrelated to raftz and should ideally be in separate PRs:

- **`.golangci.yml`**: Adding the `unparam` linter is orthogonal to this feature.
- **`README.md`**: Changing `--token-file` description from "Path to file
  containing a NATS token" to "Path to a file with a bearer token" is unrelated.
- **`surveyor/observation_test.go`**: Adding `ncAgg.Flush()` and `ncA.Flush()` is
  a test deflake fix unrelated to raftz.

These are individually fine changes, but mixing them makes the PR harder to review
and bisect. Consider splitting them out or at minimum acknowledging them in the PR
description.

### 2. Extra blank line in `pollRaftz()`

```go
func (sc *StatzCollector) pollRaftz() error {
	raftStats, err := sc.getRaftz(sc.nc)
	if err != nil {
		return err
	}

	sc.Lock()
	sc.raftStats = raftStats
	sc.Unlock()

	return nil
                    // <-- extra blank line here
}
```

Minor nit: there's a trailing blank line before the closing brace. Should be
removed for consistency with `pollGatewayInfo()`.

### 3. Error handling inconsistency in `getRaftz()` vs `poll()`

In `getRaftz()`, if `requestMany()` fails, the error is logged as a warning but
execution continues (returns whatever partial results were collected):

```go
msgs, err := requestMany(nc, sc, subj, reqJSON, true)
if err != nil {
    sc.logger.Warnf("Error requesting raftz stats: %s", err.Error())
}
```

This is fine in isolation, but `pollRaftz()` is called from `poll()` and returns
a hard error:

```go
if sc.collectRaftz {
    err := sc.pollRaftz()
    if err != nil {
        return err
    }
}
```

Since `getRaftz()` only returns `nil` error (the `json.Marshal` on a simple struct
should never fail), the error return path from `pollRaftz()` is effectively dead
code. Consider either:
- Making `getRaftz()` actually propagate the `requestMany` error when no results
  are returned (i.e., `if err != nil && len(msgs) == 0 { return nil, err }`), or
- Removing the error return from `pollRaftz()` entirely (like a void function
  that just logs warnings internally).

The current code technically works but the error flow is misleading.

### 4. Hardcoded `"_meta_"` group key in `Collect()`

```go
for _, group := range *(raftStat.Data) {
    meta, ok := group["_meta_"]
    if !ok {
        continue
    }
```

The `getRaftz()` already filters with `GroupFilter: "_meta_"`, so receiving data
for any other group would be unexpected. However, the iteration + map lookup
pattern is defensive and reasonable.

That said, if the server-side API changes the key name or structure in the future,
this will silently produce no metrics. A log warning when `_meta_` is missing
would aid debugging:

```go
if !ok {
    sc.logger.Debugf("raftz response missing _meta_ group for server %s", raftStat.Server.Name)
    continue
}
```

### 5. Deprecated `NewStatzCollector` signature growing

Adding yet another positional boolean (`raftz bool`) to the already long
deprecated `NewStatzCollector()` function makes it increasingly fragile. The
signature now has 15 parameters. I see this function is already marked deprecated
in favor of `NewStatzCollectorOpts`, but it continues to accumulate parameters.

Consider whether this deprecated function should stop gaining new parameters.
Callers using it (like in `surveyor.go:createStatszCollector`) could be migrated
to `NewStatzCollectorOpts` instead, which would be a cleaner long-term approach.
Not a blocker for this PR, but worth noting.

### 6. Test coverage is minimal

`TestSurveyor_Raftz` only checks that the three metric names appear in the
`/metrics` output. It doesn't verify:
- That metric values are non-zero / sensible
- That per-server labels are present
- Behavior when `--raftz` is not set (metrics should be absent)

The gatewayz test (`TestSurveyor_Gatewayz`) similarly only checks for metric name
presence, so this is consistent with existing patterns, but it would be nice to
have slightly more robust validation.

---

## Dashboard Review

The Grafana dashboard changes look good:
- Terminology correction from "Metalayer" to "Meta layer" is welcome
- New panels for "Uncommitted entries" (`pindex - committed`) and "Unapplied
  entries" (`committed - applied`) are useful operational indicators
- Tooltip and legend improvements (multi mode, desc sort) improve usability
- Added descriptions provide good context for operators

---

## Positive Observations

- Clean separation following the established collector pattern
- Good use of functional options (`WithCollectRaftz`)
- `raftStat` struct correctly embeds `ServerAPIResponse` for consistent
  deserialization
- The `GroupFilter: "_meta_"` parameter to `RaftzOptions` correctly scopes the
  request
- Dashboard additions are operationally valuable

## Verdict

**Approve with minor comments.** The core implementation is solid and follows
project conventions. The main feedback is:
1. Split out unrelated changes if possible
2. Fix the trailing blank line
3. Consider improving error flow clarity in `getRaftz()`/`pollRaftz()`
4. Optionally add a debug log for missing `_meta_` group
