# Analysis Tab — Development Plan

Planned enhancements for the Analysis tab, in priority order.

---

## 1. Multi-Select via Cmd/Ctrl+Click

**Status:** Planned

### Goal

Allow users to build up a selection by holding Cmd (macOS) or Ctrl (Windows/Linux) while clicking scatter points or histogram bars, rather than replacing the current selection on each click.

### Current Behavior

- Clicking a scatter point sets `gallery_filter_mode = "points"` and replaces `gallery_filter_ids` with the new point's ID.
- Clicking a histogram bar sets `gallery_filter_mode = "bin"` and replaces `gallery_filter_value` with the new bin range.
- Each click discards the previous selection.

### Desired Behavior

- **Scatter + Cmd/Ctrl click:** Add the clicked point(s) to `gallery_filter_ids`. If the filter mode is currently `"bin"`, switch to `"points"` and seed with any previously bin-matched IDs before adding.
- **Histogram + Cmd/Ctrl click:** Add the bin's measurement IDs to `gallery_filter_ids`. Switch `gallery_filter_mode` to `"points"` so the combined ID set drives the gallery.
- **Plain click (no modifier):** Behavior unchanged — replaces selection.
- **Filter label:** Update to reflect a compound selection, e.g., "3 spores selected" when multiple individual points are held, or the bin range when a single bin is active.

### Implementation Notes

The entry point is `on_gallery_plot_pick()` in `main_window.py` (~line 13064). The matplotlib `pick_event` carries a `mouseevent` attribute with `key` set to `"ctrl"` or `"cmd"` (or `"super"` on some platforms) when a modifier is held.

**Scatter pick with modifier:**
```python
mouse_event = getattr(event, "mouseevent", None)
modifiers = getattr(mouse_event, "key", "") or ""
is_additive = "ctrl" in modifiers or "super" in modifiers or "meta" in modifiers

if is_additive:
    self.gallery_filter_ids |= selected_ids   # union, not replace
else:
    self.gallery_filter_ids = selected_ids
self.gallery_filter_mode = "points"
```

**Histogram pick with modifier:**

Histogram patches currently store `(metric, min_val, max_val)`. To support additive bin selection, resolve the bin's matching measurement IDs at pick time and add them to `gallery_filter_ids`.

```python
if hasattr(self, "gallery_hist_patches") and event.artist in self.gallery_hist_patches:
    metric, min_val, max_val = self.gallery_hist_patches[event.artist]
    modifiers = getattr(getattr(event, "mouseevent", None), "key", "") or ""
    is_additive = "ctrl" in modifiers or "super" in modifiers or "meta" in modifiers

    if is_additive:
        # Resolve IDs in this bin and union with existing selection
        bin_ids = self._ids_in_bin(metric, min_val, max_val)
        self.gallery_filter_ids |= bin_ids
        self.gallery_filter_mode = "points"
    else:
        self.gallery_filter_mode = "bin"
        self.gallery_filter_value = (metric, min_val, max_val)
        self.gallery_filter_ids = set()
```

A helper `_ids_in_bin(metric, min_val, max_val)` iterates the current gallery measurements and returns IDs where the relevant dimension falls within `[min_val, max_val]`.

**Filter label:** Update `_update_gallery_filter_label()` to show a count when mode is `"points"` with multiple IDs:
```python
if self.gallery_filter_mode == "points" and self.gallery_filter_ids:
    n = len(self.gallery_filter_ids)
    label = self.tr("%n spore(s) selected", n)
```

### Acceptance Criteria

- Cmd/Ctrl+click on a second scatter point adds it to the gallery without deselecting the first.
- Cmd/Ctrl+click on a histogram bar adds that bin's spores to any existing point selection.
- Plain click still replaces the selection.
- "Clear filter" resets the combined selection.
- Filter label reflects the compound state.

---

*Further planned features will be added here.*
