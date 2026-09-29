# LIFELINE landing page and dashboard route

## Build
- Replace `/` with a concise LIFELINE landing page using a fixed translucent navigation, centered product-led introduction, one “Open dashboard” action, and an original code-native dashboard preview.
- Move the existing complete operational interface to `/dashboard` without changing its API client, polling, fixtures, controls, or live/design-preview separation.
- Add landing sections for how the edge-to-shard-to-server flow works, grounded capabilities, and privacy controls; use in-page anchors only for these landing sections.
- Keep navigation honest: no sign-in controls, fabricated metrics, stock imagery, external template assets, or unsupported claims.

## Design
- Extend the graphite/navy token system with restrained cyan highlights, translucent surfaces, fine borders, controlled reflections, and subtle depth.
- Add small hover lifts and border brightening to interactive surfaces while retaining visible focus states and dense dashboard readability.
- Reduce blur and spacing appropriately on mobile, and disable all movement under reduced-motion preferences.

## Technical details
- Create a focused landing component and a `/dashboard` route, then make `/` render only the landing page.
- Use TanStack links for route navigation and preserve the central backend boundary in `src/features/lifeline/api.ts` unchanged.
- Give `/` and `/dashboard` distinct titles, descriptions, Open Graph metadata, and Twitter card metadata.
- Update project documentation and architecture notes to reflect the two-route product structure.
- Validate the production build and inspect desktop and mobile layouts, including the dashboard’s preview mode.

## Assumptions
- The dashboard preview on the landing page is a lightweight static representation built from labelled product concepts, not live data or invented telemetry.
- “Fixed navigation” applies to the landing page; the operational dashboard retains its purpose-built sticky controls.
