"""Render median drain times from evaluate_server_admission.py's JSON output."""

import json
import sys
from pathlib import Path


def render(evaluation):
    rows = [
        ("Aggregate / 16 in flight", "aggregate"),
        ("Independent / 1,000 senders", "independent"),
        ("Independent / 1,000 + tenant cap", "candidate"),
    ]
    medians = evaluation["medians"]
    longest = max(medians[key]["elapsed_seconds"] for _, key in rows)
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 600 268" role="img" aria-labelledby="title desc">',
        '<title id="title">Time to drain 360,000 events</title>',
        '<desc id="desc">Median of five repetitions per condition. Shorter bars indicate faster recovery.</desc>',
        '<rect width="600" height="268" rx="8" fill="#f6f8fa"/>',
        '<g font-family="system-ui, sans-serif" fill="#182330">',
        '<text x="18" y="32" font-size="21" font-weight="bold">Time to drain 360,000 events</text>',
        '<text x="18" y="55" font-size="14">Server admission study / median of five runs / lower is better</text>',
    ]
    for i, (label, key) in enumerate(rows):
        seconds = medians[key]["elapsed_seconds"]
        width = 495 * seconds / longest
        y = 88 + 63 * i
        parts.extend([
            f'<text x="18" y="{y}" font-size="16">{label}</text>',
            f'<rect x="18" y="{y + 9}" width="{width:.2f}" height="16" fill="#246581"/>',
            f'<text x="{26 + width:.2f}" y="{y + 23}" font-size="15">{seconds:.2f} s</text>',
        ])
    return "\n".join(parts + ["</g>", "</svg>", ""])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Usage: render_drain.py evaluation.json output.svg")
    result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    Path(sys.argv[2]).write_bytes(render(result).encode("utf-8"))
