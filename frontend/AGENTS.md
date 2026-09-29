<!-- LOVABLE:BEGIN -->
> [!IMPORTANT]
> This project is connected to [Lovable](https://lovable.dev). Avoid rewriting
> published git history — force pushing, or rebasing/amending/squashing commits
> that are already pushed — as it rewrites history on Lovable's side and the
> user will likely lose their project history.
>
> Commits you push to the connected branch sync back to Lovable and show up in
> the editor, so keep the branch in a working state.
<!-- LOVABLE:END -->

## Project architecture
- Keep all backend communication behind `src/features/lifeline/api.ts`; this preserves a typed, replaceable API boundary and strict live/demo separation.
- Keep `/` as the public product landing page and `/dashboard` as the complete operational console; this separates product context from dense operations without duplicating behavior.
- Keep the landing hero’s generated visual in `src/components/ui/halftone-nebula.tsx` as a self-contained WebGL2 component with a static fallback; this preserves the requested interaction while keeping product data separate.
