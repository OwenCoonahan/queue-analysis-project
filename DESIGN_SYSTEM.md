# Design System

A precision design system for data-intensive applications. Inspired by Linear, Palantir, Bloomberg.

---

## Principles

1. **Data density** — Show more, scroll less. Information-rich without clutter
2. **Precision** — Every pixel intentional. Tight spacing, clean alignment
3. **Speed** — Fast interactions, keyboard-first, minimal latency feel
4. **Clarity** — Clear hierarchy. Eyes go where they should
5. **Professional** — Serious tools for serious work. No decoration

---

## Color Modes

The system supports both light and dark modes. Dark mode is recommended for data-heavy dashboards (reduces eye strain, makes data pop).

### Light Mode

| Token | Hex | Usage |
|-------|-----|-------|
| `--bg-base` | `#FFFFFF` | Primary background |
| `--bg-subtle` | `#F8F9FA` | Secondary surfaces |
| `--bg-muted` | `#F1F3F5` | Tertiary, hover states |
| `--bg-emphasis` | `#E9ECEF` | Active states, selection |
| `--text-primary` | `#111827` | Headings, primary content |
| `--text-secondary` | `#4B5563` | Body text, descriptions |
| `--text-muted` | `#9CA3AF` | Metadata, timestamps |
| `--text-faint` | `#D1D5DB` | Placeholders, disabled |
| `--border-default` | `#E5E7EB` | Cards, inputs |
| `--border-subtle` | `#F3F4F6` | Dividers, table rows |

### Dark Mode (Recommended for Dashboards)

| Token | Hex | Usage |
|-------|-----|-------|
| `--bg-base` | `#0A0A0B` | Primary background |
| `--bg-subtle` | `#111113` | Secondary surfaces |
| `--bg-muted` | `#1A1A1D` | Tertiary, hover states |
| `--bg-emphasis` | `#252529` | Active states, selection |
| `--text-primary` | `#F9FAFB` | Headings, primary content |
| `--text-secondary` | `#A1A1AA` | Body text, descriptions |
| `--text-muted` | `#71717A` | Metadata, timestamps |
| `--text-faint` | `#3F3F46` | Placeholders, disabled |
| `--border-default` | `#27272A` | Cards, inputs |
| `--border-subtle` | `#1F1F23` | Dividers, table rows |

### Accent Colors

Single accent for actions. Keep it minimal.

| Token | Light | Dark | Usage |
|-------|-------|------|-------|
| `--accent` | `#2563EB` | `#3B82F6` | Primary actions |
| `--accent-hover` | `#1D4ED8` | `#2563EB` | Hover state |
| `--accent-muted` | `#DBEAFE` | `#1E3A5F` | Accent backgrounds |

### Data Palette — Sequential

For values, intensity, heatmaps. Single-hue progression.

```
Light: #EFF6FF → #BFDBFE → #60A5FA → #2563EB → #1E40AF → #1E3A8A
Dark:  #1E3A5F → #1E40AF → #2563EB → #3B82F6 → #60A5FA → #93C5FD
```

### Data Palette — Categorical

For distinct categories. Maximum 6 colors. If more needed, use patterns or faceting.

| Token | Hex | Name |
|-------|-----|------|
| `--cat-1` | `#2563EB` | Blue |
| `--cat-2` | `#059669` | Emerald |
| `--cat-3` | `#D97706` | Amber |
| `--cat-4` | `#DC2626` | Red |
| `--cat-5` | `#7C3AED` | Violet |
| `--cat-6` | `#475569` | Slate |

### Status Colors

| Token | Color | Light BG | Usage |
|-------|-------|----------|-------|
| `--positive` | `#10B981` | `#D1FAE5` | Success, gains, online |
| `--negative` | `#EF4444` | `#FEE2E2` | Error, losses, offline |
| `--warning` | `#F59E0B` | `#FEF3C7` | Caution, pending |
| `--info` | `#3B82F6` | `#DBEAFE` | Informational |

---

## Typography

### Font Stack

```css
--font-sans: 'Geist', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
--font-mono: 'JetBrains Mono', 'SF Mono', 'Fira Code', monospace;
```

Use **Geist** for UI. Use **monospace** for numbers in tables/metrics (better alignment).

### Type Scale

Compact scale for data density. Avoid large text except for key metrics.

| Token | Size | Weight | Line Height | Usage |
|-------|------|--------|-------------|-------|
| `--text-xs` | 11px | 400 | 16px | Labels, captions, table headers |
| `--text-sm` | 13px | 400 | 20px | Body text, table cells, most UI |
| `--text-base` | 14px | 400 | 22px | Primary body (sparingly) |
| `--text-lg` | 16px | 500 | 24px | Section titles |
| `--text-xl` | 20px | 600 | 28px | Page titles |
| `--text-2xl` | 28px | 600 | 36px | Dashboard headers |
| `--text-metric` | 32px | 600 | 40px | Key metrics, big numbers |

### Numeric Typography

Numbers in data applications need special treatment:

```css
.numeric {
  font-family: var(--font-mono);
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.01em;
}
```

### Number Formatting

| Range | Format | Example |
|-------|--------|---------|
| < 1,000 | Full number | 847 |
| 1,000 - 999,999 | Comma separated | 12,450 |
| 1M - 999M | M suffix | 2.4M |
| 1B+ | B suffix | 1.2B |
| Percentages | 1 decimal | 12.4% |
| Currency | 2 decimals | $1,234.56 |
| Deltas | +/- prefix, color | +12.4% (green) |

---

## Spacing

4px base unit. Keep it tight for data density.

| Token | Value | Usage |
|-------|-------|-------|
| `--space-0` | 0 | Reset |
| `--space-1` | 4px | Inline gaps, icon spacing |
| `--space-2` | 8px | Compact padding, tight gaps |
| `--space-3` | 12px | Standard padding |
| `--space-4` | 16px | Component gaps |
| `--space-5` | 20px | Section padding |
| `--space-6` | 24px | Card padding |
| `--space-8` | 32px | Section gaps |
| `--space-10` | 40px | Page margins |

### Density Modes

| Mode | Base multiplier | Use case |
|------|-----------------|----------|
| Compact | 0.75x | Data tables, trading views |
| Default | 1x | Standard dashboards |
| Comfortable | 1.25x | Reading-focused, mobile |

---

## Border & Radius

Subtle borders. Minimal radius for professional feel.

| Token | Value | Usage |
|-------|-------|-------|
| `--radius-sm` | 4px | Badges, small elements |
| `--radius-md` | 6px | Buttons, inputs, cards |
| `--radius-lg` | 8px | Modals, large containers |

Avoid heavy rounded corners. Keep it sharp and professional.

---

## Shadows

Minimal shadows. Use sparingly — prefer borders.

| Token | Value | Usage |
|-------|-------|-------|
| `--shadow-sm` | `0 1px 2px rgba(0,0,0,0.05)` | Subtle lift |
| `--shadow-md` | `0 4px 12px rgba(0,0,0,0.08)` | Dropdowns, popovers |
| `--shadow-lg` | `0 8px 24px rgba(0,0,0,0.12)` | Modals |

In dark mode, reduce shadow opacity by 50%.

---

## Components

### Buttons

Compact buttons. No heavy padding.

**Primary**
```css
.btn-primary {
  background: var(--accent);
  color: white;
  font-size: 13px;
  font-weight: 500;
  padding: 6px 12px;
  border-radius: 6px;
  border: none;
}
```

**Secondary**
```css
.btn-secondary {
  background: transparent;
  color: var(--text-secondary);
  border: 1px solid var(--border-default);
  /* same sizing as primary */
}
```

**Ghost**
```css
.btn-ghost {
  background: transparent;
  color: var(--text-muted);
  border: none;
  /* same sizing */
}
```

### Inputs

```css
.input {
  background: var(--bg-base);
  border: 1px solid var(--border-default);
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 13px;
  color: var(--text-primary);
}

.input:focus {
  border-color: var(--accent);
  outline: none;
  box-shadow: 0 0 0 2px var(--accent-muted);
}
```

### Cards

```css
.card {
  background: var(--bg-base);
  border: 1px solid var(--border-default);
  border-radius: 6px;
  padding: 16px;
}
```

### Tables (High Density)

```css
.table-compact th {
  font-size: 11px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  color: var(--text-muted);
  padding: 8px 12px;
  text-align: left;
  border-bottom: 1px solid var(--border-default);
}

.table-compact td {
  font-size: 13px;
  font-family: var(--font-mono); /* for numeric columns */
  padding: 6px 12px;
  border-bottom: 1px solid var(--border-subtle);
}

.table-compact tr:hover {
  background: var(--bg-muted);
}
```

### Badges

```css
.badge {
  font-size: 11px;
  font-weight: 500;
  padding: 2px 6px;
  border-radius: 4px;
  background: var(--bg-muted);
  color: var(--text-secondary);
}

.badge-positive { background: #D1FAE5; color: #065F46; }
.badge-negative { background: #FEE2E2; color: #991B1B; }
.badge-warning { background: #FEF3C7; color: #92400E; }
```

### Metric Cards

```css
.metric {
  padding: 16px;
  border: 1px solid var(--border-default);
  border-radius: 6px;
}

.metric-label {
  font-size: 11px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  color: var(--text-muted);
  margin-bottom: 4px;
}

.metric-value {
  font-size: 32px;
  font-weight: 600;
  font-family: var(--font-mono);
  font-variant-numeric: tabular-nums;
  color: var(--text-primary);
  letter-spacing: -0.02em;
}

.metric-delta {
  font-size: 13px;
  font-family: var(--font-mono);
  margin-top: 4px;
}

.metric-delta.positive { color: var(--positive); }
.metric-delta.negative { color: var(--negative); }
```

---

## Data Visualization

### Chart Principles

1. **Remove chart junk** — No unnecessary gridlines, borders, backgrounds
2. **Direct labeling** — Label data points, not legends when possible
3. **Monospace numbers** — Align numeric axes properly
4. **Subtle axes** — Axes should recede, data should pop
5. **Consistent palette** — Use categorical colors in order

### Chart Typography

| Element | Size | Color | Font |
|---------|------|-------|------|
| Chart title | 14px, 500 | text-primary | Sans |
| Axis labels | 11px, 400 | text-muted | Sans |
| Axis values | 11px, 400 | text-muted | Mono |
| Data labels | 11px, 500 | text-secondary | Mono |
| Legend | 11px, 400 | text-muted | Sans |

### Gridlines

- Color: `var(--border-subtle)`
- Horizontal only (usually)
- No vertical gridlines unless time series
- Consider removing if data is labeled directly

### Tooltips

```css
.chart-tooltip {
  background: var(--bg-base);
  border: 1px solid var(--border-default);
  border-radius: 6px;
  padding: 8px 12px;
  box-shadow: var(--shadow-md);
  font-size: 13px;
}
```

### Altair Theme

```python
import altair as alt

def register_theme():
    return {
        'config': {
            'background': 'transparent',
            'padding': 0,
            'title': {
                'font': 'Geist',
                'fontSize': 14,
                'fontWeight': 500,
                'color': '#111827',
                'anchor': 'start',
                'offset': 12,
            },
            'axis': {
                'labelFont': 'JetBrains Mono',
                'labelFontSize': 11,
                'labelColor': '#9CA3AF',
                'titleFont': 'Geist',
                'titleFontSize': 11,
                'titleColor': '#4B5563',
                'titlePadding': 12,
                'gridColor': '#F3F4F6',
                'gridWidth': 1,
                'domainColor': '#E5E7EB',
                'domainWidth': 1,
                'tickColor': '#E5E7EB',
            },
            'legend': {
                'labelFont': 'Geist',
                'labelFontSize': 11,
                'labelColor': '#9CA3AF',
                'titleFont': 'Geist',
                'titleFontSize': 11,
                'titleColor': '#4B5563',
                'symbolSize': 64,
            },
            'range': {
                'category': ['#2563EB', '#059669', '#D97706', '#DC2626', '#7C3AED', '#475569'],
            },
            'view': {
                'stroke': 'transparent',
            },
            'bar': {
                'cornerRadiusTopLeft': 2,
                'cornerRadiusTopRight': 2,
            },
            'line': {
                'strokeWidth': 2,
            },
            'point': {
                'size': 48,
            },
        }
    }

alt.themes.register('custom', register_theme)
alt.themes.enable('custom')
```

### Altair Dark Theme

```python
def register_dark_theme():
    return {
        'config': {
            'background': 'transparent',
            'title': {
                'font': 'Geist',
                'fontSize': 14,
                'fontWeight': 500,
                'color': '#F9FAFB',
                'anchor': 'start',
            },
            'axis': {
                'labelFont': 'JetBrains Mono',
                'labelFontSize': 11,
                'labelColor': '#71717A',
                'gridColor': '#27272A',
                'domainColor': '#3F3F46',
            },
            'range': {
                'category': ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#94A3B8'],
            },
            'view': {'stroke': 'transparent'},
        }
    }
```

---

## Animation & Motion

Keep animations subtle and fast. No bouncy or playful effects.

| Property | Duration | Easing |
|----------|----------|--------|
| Hover states | 100ms | ease-out |
| Transitions | 150ms | ease-out |
| Page transitions | 200ms | ease-in-out |
| Skeleton loading | 1.5s loop | ease-in-out |

```css
.transition-default {
  transition: all 150ms ease-out;
}

.transition-fast {
  transition: all 100ms ease-out;
}
```

### Loading States

- Use skeleton loaders, not spinners
- Match skeleton to actual content dimensions
- Subtle pulse animation

---

## Keyboard Shortcuts

Data apps should be keyboard-navigable. Common patterns:

| Key | Action |
|-----|--------|
| `/` | Focus search |
| `g` then `d` | Go to dashboard |
| `g` then `p` | Go to projects |
| `j` / `k` | Navigate list down/up |
| `Enter` | Open/select |
| `Esc` | Close modal, clear selection |
| `?` | Show keyboard shortcuts |

Display shortcuts in tooltips: `Export ⌘E`

---

## Layout Patterns

### Dashboard Grid

```css
.dashboard-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 16px;
}
```

### Sidebar Layout

```css
.layout-sidebar {
  display: grid;
  grid-template-columns: 240px 1fr;
  min-height: 100vh;
}

.sidebar {
  background: var(--bg-subtle);
  border-right: 1px solid var(--border-default);
  padding: 16px;
}
```

### Data Table with Fixed Header

```css
.table-container {
  max-height: 600px;
  overflow-y: auto;
}

.table-container thead {
  position: sticky;
  top: 0;
  background: var(--bg-base);
  z-index: 10;
}
```

---

## Streamlit Implementation

```python
import streamlit as st

def apply_design_system(dark_mode=True):
    if dark_mode:
        bg_base = '#0A0A0B'
        bg_subtle = '#111113'
        bg_muted = '#1A1A1D'
        text_primary = '#F9FAFB'
        text_secondary = '#A1A1AA'
        text_muted = '#71717A'
        border = '#27272A'
    else:
        bg_base = '#FFFFFF'
        bg_subtle = '#F8F9FA'
        bg_muted = '#F1F3F5'
        text_primary = '#111827'
        text_secondary = '#4B5563'
        text_muted = '#9CA3AF'
        border = '#E5E7EB'

    st.markdown(f"""
    <style>
    @import url('https://cdn.jsdelivr.net/npm/geist@1.2.0/dist/fonts/geist-sans/style.min.css');
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap');

    :root {{
        --bg-base: {bg_base};
        --bg-subtle: {bg_subtle};
        --bg-muted: {bg_muted};
        --text-primary: {text_primary};
        --text-secondary: {text_secondary};
        --text-muted: {text_muted};
        --border: {border};
        --accent: #2563EB;
    }}

    html, body, [class*="css"] {{
        font-family: 'Geist', -apple-system, system-ui, sans-serif;
        color: var(--text-primary);
    }}

    .stApp {{
        background: var(--bg-base);
    }}

    /* Metrics */
    [data-testid="stMetric"] {{
        background: var(--bg-subtle);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 16px;
    }}

    [data-testid="stMetricValue"] {{
        font-family: 'JetBrains Mono', monospace;
        font-variant-numeric: tabular-nums;
    }}

    /* Headers */
    h1, h2, h3 {{
        color: var(--text-primary);
        letter-spacing: -0.02em;
    }}

    /* Tables */
    .stDataFrame {{
        border: 1px solid var(--border);
        border-radius: 6px;
    }}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background: var(--bg-subtle);
        border-right: 1px solid var(--border);
    }}

    /* Buttons */
    .stButton > button {{
        background: var(--accent);
        color: white;
        border: none;
        border-radius: 6px;
        font-weight: 500;
        padding: 8px 16px;
    }}

    .stButton > button:hover {{
        background: #1D4ED8;
    }}
    </style>
    """, unsafe_allow_html=True)
```

---

## File Conventions

| Type | Convention | Example |
|------|------------|---------|
| CSS variables | `--kebab-case` | `--text-primary` |
| CSS classes | `.kebab-case` | `.metric-card` |
| Python files | `snake_case.py` | `data_viz.py` |
| React components | `PascalCase.tsx` | `MetricCard.tsx` |

---

## Accessibility

- **Contrast**: Minimum 4.5:1 for text, 3:1 for UI elements
- **Focus**: Always visible focus rings (2px, accent color)
- **Touch targets**: Minimum 44x44px on mobile
- **Motion**: Respect `prefers-reduced-motion`
- **Color**: Never use color alone to convey meaning

---

## Quick Reference

### CSS Variables (Dark Mode)

```css
:root {
  /* Background */
  --bg-base: #0A0A0B;
  --bg-subtle: #111113;
  --bg-muted: #1A1A1D;
  --bg-emphasis: #252529;

  /* Text */
  --text-primary: #F9FAFB;
  --text-secondary: #A1A1AA;
  --text-muted: #71717A;

  /* Border */
  --border-default: #27272A;
  --border-subtle: #1F1F23;

  /* Accent */
  --accent: #3B82F6;
  --accent-hover: #2563EB;

  /* Status */
  --positive: #10B981;
  --negative: #EF4444;
  --warning: #F59E0B;

  /* Data */
  --cat-1: #3B82F6;
  --cat-2: #10B981;
  --cat-3: #F59E0B;
  --cat-4: #EF4444;
  --cat-5: #8B5CF6;
  --cat-6: #94A3B8;

  /* Typography */
  --font-sans: 'Geist', system-ui, sans-serif;
  --font-mono: 'JetBrains Mono', monospace;

  /* Spacing */
  --space-1: 4px;
  --space-2: 8px;
  --space-3: 12px;
  --space-4: 16px;
  --space-6: 24px;
  --space-8: 32px;

  /* Radius */
  --radius-sm: 4px;
  --radius-md: 6px;
  --radius-lg: 8px;
}
```
