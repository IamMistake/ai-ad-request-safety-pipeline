# AI Ad Request Safety Pipeline Design System

## Direction

The site presents a security/data pipeline as a polished safety-systems product page: bright, technical, trustworthy, and dimensional without turning into generic blue-purple SaaS. The memorable moment is the hero's layered request-inspection stack: raw request, stream routing, risk scoring, and content safety shown as lit operational layers.

## Tokens

- Canvas: `#fbfcff`, `#f6f8ff`, `#edf4ff`
- Ink: `#0f172a`, `#243044`, `#64748b`
- Primary: `#2f46d8`, `#5268ff`, `#8fa4ff`
- Signal: `#00a6d6`, `#14b8a6`, `#f59e0b`, `#e11d48`
- Surface: translucent white with 1px cool borders, blue-tinted shadows, subtle mesh/glow backgrounds
- Typography: Manrope for display/body, JetBrains Mono for metrics/code

## Components

- Floating Contents rail: compact glass surface, active section gradient, explicit `Contents` label.
- Hero buttons: dark primary, glass secondary, transform-only hover/active states.
- Metric cards: white glass cards with tinted shadows and tabular numeric display.
- Safety stack card: layered glass panels, status chips, and signal rails to create the hero focal object.
- Diagram/cards/tables: rounded technical cards with one consistent light direction and restrained accent colors.
- Hero metadata row: centered/link-forward author, affiliation, contribution, and contact line in the academic-project-page style.
- Compact diagram frame: large source diagrams render inside centered, max-width figure cards instead of occupying the full content width.
- Interactive pipeline view: dark architecture-shaped branch map matching the Excalidraw flow: ingress across the top, Flink verdict branches to `requests.sus`, `requests.clean`, and `requests.fraud`, RFC/moderation/ad-injection continuation, blocked fraud store, and Spark model-training support.

## Motion and States

- Motion only communicates affordance or entrance. Use `transform` and `opacity`; no layout-property animation.
- Hover states lift or illuminate interactive controls. Focus states must remain visible.
- Reduced-motion users receive static visible content.
- Pipeline animation units use `translateY(-8px) scale(1.05)` plus indigo/cyan glow to show the active branch stage.
- Connector pulses travel slowly for `1.45s ease-in-out`; scenario badges bump when Clean, Fraud, and Suspicious request paths begin.
- Trace output uses JetBrains Mono, green cursor blink, and short streamed status lines for exactly three request paths: clean approval, fraud block, and suspicious RFC resolution.
