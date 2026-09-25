#!/usr/bin/env python3
"""Render reproducible release-comparison charts from fyntr-bench trial JSON."""

import argparse
import statistics
from pathlib import Path

import compare


NORMAL_METRICS = (
    ("throughput_bytes_per_second", "Throughput"),
    ("fyntr_cpu_ns_per_transferred_byte", "Fyntr CPU ns/B"),
    ("fyntr_peak_rss_bytes", "Peak RSS"),
    ("p99_latency_ms", "p99 latency"),
)


def setting_name(key):
    return (
        f"{key[0]} (flows={key[1]}, bulk={key[2]} B, "
        f"warmup={key[5]} s, measurement={key[6]} s)"
    )


def paired_groups(pairs):
    """Load pairs and reject settings that cannot be compared one-for-one."""
    result = {}
    for old_path, new_path in pairs:
        old_groups = compare.groups(compare.load(old_path))
        new_groups = compare.groups(compare.load(new_path))
        validate_group_sets(old_groups, new_groups, old_path, new_path)
        for key in old_groups:
            validate_trial_ids(old_groups[key], new_groups[key], key, old_path, new_path)
            if key in result:
                raise ValueError(
                    f"duplicate scenario settings across --pair arguments: {setting_name(key)}"
                )
            result[key] = (old_groups[key], new_groups[key])
    if not result:
        raise ValueError("no benchmark JSON trials found")
    return result


def validate_group_sets(old_groups, new_groups, old_path="old", new_path="new"):
    old_keys, new_keys = set(old_groups), set(new_groups)
    if old_keys == new_keys:
        return
    only_old = sorted(old_keys - new_keys, key=str)
    only_new = sorted(new_keys - old_keys, key=str)
    details = []
    if only_old:
        details.append("only in old: " + ", ".join(setting_name(key) for key in only_old))
    if only_new:
        details.append("only in new: " + ", ".join(setting_name(key) for key in only_new))
    raise ValueError(
        f"cannot compare {old_path} with {new_path}; " + "; ".join(details)
    )


def validate_trial_ids(old_rows, new_rows, key, old_path="old", new_path="new"):
    def ids(rows, side):
        result = []
        missing = []
        for position, row in enumerate(rows, start=1):
            trial = row.get("trial")
            if trial is None:
                missing.append(position)
            else:
                result.append(trial)
        if missing:
            raise ValueError(
                f"cannot compare {old_path} with {new_path} for {setting_name(key)}; "
                f"{side} trial IDs are missing at row positions {missing}"
            )
        duplicates = sorted({trial for trial in result if result.count(trial) > 1}, key=str)
        if duplicates:
            raise ValueError(
                f"cannot compare {old_path} with {new_path} for {setting_name(key)}; "
                f"{side} trial IDs are not unique: {duplicates}"
            )
        return set(result)

    old_ids = ids(old_rows, "old")
    new_ids = ids(new_rows, "new")
    if old_ids != new_ids:
        raise ValueError(
            f"cannot compare {old_path} with {new_path} for {setting_name(key)}; "
            f"old trial IDs {sorted(old_ids, key=str)}, "
            f"new trial IDs {sorted(new_ids, key=str)}"
        )


def values(rows, field, nested=None):
    result = []
    for row in rows:
        value = (row.get(nested) or {}).get(field) if nested else row.get(field)
        if value is not None:
            result.append(float(value))
    return result


def normalized_pair(old_rows, new_rows, field, context="scenario"):
    """Return each observation as a percentage of its matching old median."""
    old_values = values(old_rows, field)
    new_values = values(new_rows, field)
    if not old_values or not new_values:
        return None
    baseline = statistics.median(old_values)
    if baseline == 0:
        raise ValueError(
            f"cannot normalize {context}: old median for {field} is zero"
        )
    return ([value / baseline * 100 for value in old_values], [value / baseline * 100 for value in new_values])


def groups_with_metric(groups, field, nested=None):
    """Keep only scenarios where both versions observed a charted metric."""
    return [
        (key, old_rows, new_rows)
        for key, (old_rows, new_rows) in groups
        if values(old_rows, field, nested) and values(new_rows, field, nested)
    ]


def recovery_count(rows):
    return sum(bool((row.get("s7") or {}).get("recovered")) for row in rows)


def _mpl():
    try:
        import matplotlib.pyplot as pyplot
    except ModuleNotFoundError as error:
        raise SystemExit(
            "matplotlib is required only for rendering; run with "
            "`uv run --with matplotlib python bench/report/plot.py ...`"
        ) from error
    return pyplot


def _scatter_pair(axis, x, old_values, new_values, old_label, new_label, *, scale=1.0):
    old_x, new_x = x - 0.17, x + 0.17
    old_scaled = [value / scale for value in old_values]
    new_scaled = [value / scale for value in new_values]
    axis.scatter([old_x] * len(old_scaled), old_scaled, color="#4C78A8", alpha=0.72, label=old_label)
    axis.scatter([new_x] * len(new_scaled), new_scaled, color="#F58518", alpha=0.72, label=new_label)
    axis.hlines(statistics.median(old_scaled), old_x - 0.11, old_x + 0.11, color="#1F4E79", linewidth=2.5)
    axis.hlines(statistics.median(new_scaled), new_x - 0.11, new_x + 0.11, color="#B95B00", linewidth=2.5)


def _record_observations(axis, x, rows, color, label, marker):
    observed = values(rows, "generated_records", "s7"), values(rows, "acknowledged_records", "s7")
    for offset, record_values, suffix, marker_style in (
        (-0.11, observed[0], "generated", "o"),
        (0.11, observed[1], "acknowledged", "^"),
    ):
        if not record_values:
            continue
        axis.scatter(
            [x + offset] * len(record_values), record_values, color=color, alpha=0.72,
            marker=marker_style if marker is None else marker, label=f"{label} {suffix}",
        )
        axis.hlines(
            statistics.median(record_values), x + offset - 0.07, x + offset + 0.07,
            color=color, linewidth=2.5,
        )


def render_normal(groups, output_dir, old_label, new_label):
    pyplot = _mpl()
    normal = [(key, rows) for key, rows in groups.items() if key[0] != "S7"]
    if not normal:
        return []
    normal.sort(key=lambda item: str(item[0]))
    figure, axes = pyplot.subplots(2, 2, figsize=(13, 8), constrained_layout=True)
    for axis, (field, title) in zip(axes.flat, NORMAL_METRICS):
        measured = groups_with_metric(normal, field)
        labels = [
            f"{key[0]}\n{key[1]} flow{'s' if key[1] != 1 else ''}"
            for key, _, _ in measured
        ]
        for index, (key, old_rows, new_rows) in enumerate(measured):
            pair = normalized_pair(old_rows, new_rows, field, setting_name(key))
            _scatter_pair(axis, index, *pair, old_label, new_label)
        axis.axhline(100, color="#777777", linewidth=1, linestyle="--")
        axis.set_title(f"{title} (old median = 100)")
        axis.set_ylabel("relative to matching old median (%)")
        axis.set_xticks(range(len(labels)), labels)
        axis.grid(axis="y", alpha=0.25)
        if measured:
            handles, legend_labels = axis.get_legend_handles_labels()
            axis.legend(dict(zip(legend_labels, handles)).values(), dict(zip(legend_labels, handles)).keys(), loc="best")
        else:
            axis.text(0.5, 0.5, "not measured", ha="center", va="center", transform=axis.transAxes)
    figure.suptitle("Normal-load trial observations (points) and medians (lines)")
    return save(figure, output_dir, "normal-load")


def render_s7(groups, output_dir, old_label, new_label):
    pyplot = _mpl()
    s7 = [(key, rows) for key, rows in groups.items() if key[0] == "S7"]
    if not s7:
        return []
    s7.sort(key=lambda item: str(item[0]))
    figure, axes = pyplot.subplots(2, 2, figsize=(13, 8), constrained_layout=True)
    chart_specs = (
        (axes[0, 0], "fyntr_peak_rss_bytes", None, "Peak RSS (MiB)", 1024 * 1024),
        (axes[1, 0], "maximum_record_write_ms", "s7", "Maximum record write duration (ms)", 1),
    )
    for axis, field, nested, title, scale in chart_specs:
        measured = groups_with_metric(s7, field, nested)
        labels = [f"S7\n{key[1]} flow{'s' if key[1] != 1 else ''}" for key, _, _ in measured]
        for index, (_, old_rows, new_rows) in enumerate(measured):
            old_values = values(old_rows, field, nested)
            new_values = values(new_rows, field, nested)
            _scatter_pair(axis, index, old_values, new_values, old_label, new_label, scale=scale)
        axis.set_title(title + " — points and medians")
        axis.set_xticks(range(len(labels)), labels)
        axis.grid(axis="y", alpha=0.25)
    handles, legend_labels = axes[0, 0].get_legend_handles_labels()
    axes[0, 0].legend(dict(zip(legend_labels, handles)).values(), dict(zip(legend_labels, handles)).keys(), loc="best")

    records_axis = axes[0, 1]
    record_groups = groups_with_metric(s7, "generated_records", "s7")
    record_labels = [f"S7\n{key[1]} flow{'s' if key[1] != 1 else ''}" for key, _, _ in record_groups]
    for index, (_, old_rows, new_rows) in enumerate(record_groups):
        _record_observations(records_axis, index - 0.12, old_rows, "#4C78A8", old_label, None)
        _record_observations(records_axis, index + 0.12, new_rows, "#F58518", new_label, None)
    records_axis.set_title("Generated and acknowledged records — points and medians")
    records_axis.set_xticks(range(len(record_labels)), record_labels)
    records_axis.grid(axis="y", alpha=0.25)
    handles, legend_labels = records_axis.get_legend_handles_labels()
    records_axis.legend(dict(zip(legend_labels, handles)).values(), dict(zip(legend_labels, handles)).keys(), loc="best")

    recovery_axis = axes[1, 1]
    recovery_axis.set_ylim(0, 1.12)
    recovery_axis.set_ylabel("successful trials / total trials")
    recovery_labels = [f"S7\n{key[1]} flow{'s' if key[1] != 1 else ''}" for key, _ in s7]
    for index, (_, (old_rows, new_rows)) in enumerate(s7):
        for offset, rows, color, label in (
            (-0.17, old_rows, "#4C78A8", old_label),
            (0.17, new_rows, "#F58518", new_label),
        ):
            successes = recovery_count(rows)
            total = len(rows)
            recovery_axis.bar(index + offset, successes / total, width=0.16, color=color, alpha=0.72, label=label)
            recovery_axis.text(index + offset, successes / total + 0.035, f"{successes}/{total}", ha="center", fontsize=9)
    recovery_axis.set_title("Full recovery before deadline")
    recovery_axis.set_xticks(range(len(recovery_labels)), recovery_labels)
    recovery_axis.grid(axis="y", alpha=0.25)
    handles, legend_labels = recovery_axis.get_legend_handles_labels()
    recovery_axis.legend(dict(zip(legend_labels, handles)).values(), dict(zip(legend_labels, handles)).keys(), loc="best")
    figure.suptitle("S7 paused-backend trial observations; recovery means all generated records drained")
    return save(figure, output_dir, "s7")


def save(figure, output_dir, stem):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = [output_dir / f"{stem}.svg", output_dir / f"{stem}.png"]
    for path in paths:
        figure.savefig(path, dpi=180)
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pair", nargs=2, metavar=("OLD", "NEW"), action="append", required=True,
        help="matching old/new JSON files or directories; repeat for each scenario",
    )
    parser.add_argument("--old-label", default="old", help="legend label for old trials")
    parser.add_argument("--new-label", default="new", help="legend label for new trials")
    parser.add_argument("--output-dir", required=True, help="directory for SVG and PNG charts")
    args = parser.parse_args()
    try:
        groups = paired_groups(args.pair)
        paths = render_normal(groups, args.output_dir, args.old_label, args.new_label)
        paths += render_s7(groups, args.output_dir, args.old_label, args.new_label)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    if not paths:
        raise SystemExit("no normal-load or S7 scenarios found")
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
