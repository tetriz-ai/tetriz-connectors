#!/usr/bin/env python3
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT

"""Render an illustrated README demo from a manifest and checked output excerpts (Pillow)."""
import argparse
import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--preview-dir', type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
assets = root / '.github/assets/demo'
spec = json.loads((assets / 'scenes.json').read_text())
W, H = 1200, 820
INK, MUTED, ACCENT = '#182b37', '#526572', spec['accent']


def font(size, bold=False, mono=False):
    candidates = [
        Path('/System/Library/Fonts') / ('Menlo.ttc' if mono else f'Supplemental/Arial{" Bold" if bold else ""}.ttf'),
        Path('/usr/share/fonts/truetype/dejavu') / ('DejaVuSansMono.ttf' if mono else f'DejaVuSans{"-Bold" if bold else ""}.ttf'),
    ]
    for p in candidates:
        if p.exists():
            return ImageFont.truetype(str(p), size)
    raise SystemExit('Install Arial/Menlo (macOS) or DejaVu Sans/Mono (Linux).')


def wrap(d, value, f, width):
    lines = []
    for paragraph in value.split('\n'):
        line = ''
        for word in paragraph.split():
            candidate = f'{line} {word}'.strip()
            if d.textlength(candidate, font=f) > width and line:
                lines.append(line)
                line = word
            else:
                line = candidate
            if d.textlength(line, font=f) > width:
                raise ValueError(f'Unbreakable text exceeds layout width: {line}')
        lines.append(line)
    return lines


def block(d, value, x, y, width, size=25, color=INK, bold=False, mono=False, bottom=735):
    f = font(size, bold, mono)
    lines = wrap(d, value, f, width)
    step = size + 10
    if y + len(lines)*step > bottom:
        raise ValueError(f'Text exceeds layout height: {value}')
    for line in lines:
        d.text((x, y), line, font=f, fill=color)
        y += step
    return y


frames, durations = [], []
for i, s in enumerate(spec['scenes']):
    for evidence in s.get('evidence', []):
        source = (assets / evidence['file']).read_text()
        if evidence['excerpt'] not in source:
            raise ValueError(f"Scene {i+1} has unsupported evidence: {evidence['file']}")
    im = Image.new('RGB', (W, H), '#f5f7f8')
    d = ImageDraw.Draw(im)
    block(d, spec['brand'], 40, 26, 1030, 17, ACCENT, True)
    block(d, f'{i+1:02} / {len(spec["scenes"]):02}', 1080, 26, 100, 17, MUTED)
    block(d, s['title'], 40, 70, 1120, 39, bold=True, bottom=170)
    block(d, s['subtitle'], 40, 132, 1120, 23, MUTED, bottom=205)
    block(d, s['label'].upper(), 40, 233, 440, 18, ACCENT, True)
    metric_size = 58
    while d.textlength(s['metric'], font=font(metric_size, True)) > 438 and metric_size > 28:
        metric_size -= 1
    y = block(d, s['metric'], 40, 285, 440, metric_size, ACCENT, True)
    y = block(d, s['unit'], 40, y+10, 440, 27, bold=True)
    block(d, s['description'], 40, y+34, 430, 24, MUTED)
    d.rounded_rectangle((520, 219, 1160, 736), radius=14, fill='white', outline='#dce3e7', width=2)
    block(d, s['panel_label'].upper(), 548, 246, 588, 17, MUTED, True)
    y = 300
    for row in s['rows']:
        y = block(d, row, 548, y, 584, 23, bold=s.get('bold_rows', False), mono=s.get('mono', False)) + 16
    d.line((40, 762, 1160, 762), fill='#d5dee3')
    block(d, s['footer'], 40, 778, 1040, 15, MUTED, bottom=820)
    frames.append(im)
    durations.append(s.get('duration_ms', 4800))

out = root / '.github/assets/demo.gif'
frames[0].save(out, save_all=True, append_images=frames[1:], duration=durations, loop=0, optimize=True, disposal=2)
if args.preview_dir:
    args.preview_dir.mkdir(parents=True, exist_ok=True)
    for i, frame in enumerate(frames, 1):
        frame.save(args.preview_dir / f'scene-{i}.png')
print(f'{out}: {len(frames)} scenes, {sum(durations)/1000:.1f}s, {out.stat().st_size} bytes')
