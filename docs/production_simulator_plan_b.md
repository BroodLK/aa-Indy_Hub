# Indy Hub Production Simulator: Plan B Implementation Brief

## Mission

Make the production simulator discoverable, understandable, shareable, and trustworthy while preserving the current simulation engine. The work should make the simulator feel like a coherent production workspace rather than a collection of loosely connected tabs.

The implementation must preserve the distinction between:

- durable user preferences, which belong to the user and can be reused across simulations;
- simulation state, which belongs to one saved production simulation;
- live ESI data, which is refreshed only when the user has the necessary scope and when a refresh is useful;
- shareable scenario state, which may travel in a URL but must not expose or preload private character, asset, BPC, system, or structure choices for another user.

The repository already contains the principal surfaces:

- `indy_hub/templates/indy_hub/industry/Craft_BP_v2.html` — tab markup, help text, save/load UI, data bootstrap, and inline UI behavior;
- `indy_hub/static/indy_hub/js/craft_bp.js` — calculations, price state, buy/build behavior, export, optimization, and schedule interactions;
- `indy_hub/static/indy_hub/js/craft_bp_simulation_api.js` — simulation state and price state helpers;
- `indy_hub/static/indy_hub/js/craft_bp_owned.js` — owned material/BPC and planner behavior;
- `indy_hub/static/indy_hub/css/craft_bp.css` — simulator styling;
- `indy_hub/views/industry.py` — simulator payload creation, save/load endpoints, scheduler payload, and industry-job data;
- `indy_hub/models.py` — `ProductionSimulation`, `ProductionConfig`, `BlueprintEfficiency`, `CustomPrice`, and `IndustryJob`;
- `indy_hub/services/build_scheduler.py` — slot allocation and schedule calculation;
- `indy_hub/services/capital_price_estimates.py`, `indy_hub/services/reprocessing.py`, and `indy_hub/services/fuzzwork.py` — existing price and ore-related integrations;
- `indy_hub/tests/` — existing simulator, scheduler, material, contract, and asset tests.

Do not begin by rewriting the simulator. First map the existing state flow and keep one canonical state model for calculations, saved simulations, browser state, and shared URLs.

## Required investigation before coding

1. Read the complete simulator template, all simulator JavaScript files, and the simulator CSS. Identify every existing control, event listener, local-storage key, URL parameter, AJAX endpoint, and tab transition.
2. Trace the `production_simulations` view from request parameters through the template context. Record which values are calculated server-side and which are recalculated in JavaScript.
3. Trace save and load requests, including the current `ui_state` payload. Confirm whether all tabs, prices, BPC selections, slot choices, and input overrides are currently included.
4. Inspect migrations `0016`, `0017`, `0018`, `0123`, and any later migrations that touch simulation state. Add new migrations rather than modifying historical migrations.
5. Inspect the character, asset, BPC, structure cache, ESI token, and buyback order models/services. Determine which existing scopes can support assets, industry jobs, structures, and contract metadata.
6. Inspect the existing URL construction in `industry.py` and the JavaScript that restores state from URL parameters. Write a state field inventory before adding more parameters.
7. Inspect the existing tests named `test_production_config_ui_state.py`, `test_craft_build_slots.py`, `test_industry_jobs_payload.py`, the material exchange price tests, and the contract tests. Extend the existing test style.
8. Confirm the project’s supported browser behavior for Bootstrap popovers/tooltips, URL length limits, and the current translation conventions.

At the end of investigation, produce a short internal map in the implementation branch listing each requested behavior, its owning layer, its persistence scope, and its test location.

## Product and navigation changes

### Discoverability

1. On the Industry page, add a clearly named `Production Simulator` action beside the existing Contract Prices action.
2. Move Contract Prices into a compact Industry actions dropdown while keeping its current URL and access checks.
3. Add concise explanatory text beneath the Production Simulator label. It should explain that users can choose Buy or Produce for a blueprint tree, estimate material cost and profit, build a slot schedule, and save/share the scenario.
4. Keep the simulator reachable from the existing Blueprint Browser path, but make the link label and supporting text consistent everywhere.
5. Add a short reload explanation below the simulator tabs if the current server round trip remains necessary. The text must explain what is refreshed and why, without sounding like an error.

### Tab information architecture

1. Keep the current major areas only if each has a clear purpose: Plan, Buy, Build, Configure, Buy BPCs, How To, and any retained optimization view.
2. Investigate `Run optimized` before changing it. Document what it calculates, what inputs it uses, and whether the result is materially different from Plan. If it duplicates the planner, remove it cleanly, including its navigation, JS, styles, help text, and dead endpoints. If it is useful, rename it to describe the output and add a plain-language explanation.
3. Remove the ambiguous Plan-tab Back button next to Save and Load.
4. Remove or explain the Materials expand/collapse arrows. They must visibly change the material list or be removed.
5. Remove the repeated “used to make” cog/emoji if the same relationship is already clear from the row layout.
6. Explain material category counts, or remove them if they do not help users make a decision.
7. Explain Runs, Output, ME, and TE in the context of the selected production simulation and its owned/bought BPC coverage.
8. Explain or remove the Blueprint and Unique Material counters.
9. Rewrite How To as readable, spaced guidance with one short section per tab. Include what Optimize does, how it derives a recommendation, what data must be supplied before Compute, and which values are estimates.

## State architecture and persistence

### Canonical state object

Create or formalize a versioned client state object. It should contain, as applicable:

- schema version;
- blueprint type ID and run count;
- active tab and expanded/collapsed sections;
- Buy/Produce/Useless decisions;
- ME/TE values;
- custom real price overrides and final sale price;
- estimate/market price snapshots and source metadata;
- tax configuration and current tax result;
- shipping configuration;
- slot allocations and schedule options;
- selected BPC/BPO coverage and run assignments;
- material input source mode and manually entered owned quantities;
- buyback selections and order draft state;
- schedule tracking opt-in and tracked character assignments;
- planner preferences that are safe to share;
- display preferences that are safe to share.

Keep private identity and inventory fields in a separate private state object:

- user-owned BPC/BPO identifiers and locations;
- character identifiers and skills;
- preferred system and structure;
- asset locations, container IDs, division names, and quantities;
- ESI token-derived structure/job data;
- private saved material-hanger and BPC-hanger selections.

The shared state must never contain private IDs merely because a user generated a share URL. Use an explicit allowlist for serialization, not a denylist.

### User preferences

Add a user-scoped preference mechanism, either a dedicated model or a carefully versioned user JSON field if an established project pattern supports it. The preference record should cover:

- last used system and structure;
- tax enabled state and tax inputs;
- buy/build display preferences;
- shipping preference;
- owned-material source preference;
- material hanger/can selection;
- BPC/BPO source location selection;
- slot display and schedule preferences;
- preferred active tab where appropriate.

Preferences must be loaded as defaults, then overridden by a saved simulation, then by an explicitly shared URL state where the field is shareable. Private preferences must be ignored when another user opens a URL.

### Saved simulations

1. Save a complete 1:1 UI state snapshot, including active tab, open sections, planner decisions, custom prices, slot selections, schedule options, and other meaningful controls.
2. On load, restore the snapshot first and then refresh only live data that must be current: skills, BPC availability, assets, material availability, buyback availability, and industry jobs.
3. Preserve saved values when live refresh fails. Display a small stale-data indicator with the last successful refresh time.
4. Make `simulation_name` unique per user at the application level. Multiple users may use the same name; one user may not create or rename two simulations to the same normalized name. Decide whether uniqueness is case-insensitive and document it. Return an inline validation error without losing the current form state.
5. Keep simulation IDs user-scoped in every read, update, delete, and load endpoint.
6. Add a state schema version and migration function for old `ui_state` payloads. Loading an old simulation must not crash when fields are absent.

## URL sharing

Implement a compact, versioned share representation for safe scenario fields.

1. Define an explicit share schema and serializer/deserializer with validation, bounds, type normalization, and unknown-field tolerance.
2. Include the blueprint, runs, Buy/Produce decisions, ME/TE, safe price overrides, tax/shipping assumptions, safe slot plan, active tab, and safe planner preferences.
3. Exclude characters, skills, owned BPCs, asset quantities, material hanger/can, preferred system, preferred structure, ESI identifiers, private contract data, and any token-derived value.
4. When another user opens a URL, load their own available BPCs as checked where they own them, use their own skills and characters, and use their own local/user defaults for private configuration.
5. Make shared URLs deterministic enough that changing one safe setting updates the URL without dropping other safe settings.
6. Avoid exposing excessive data in query strings. Prefer compressed, URL-safe JSON or a server-side share token if the encoded state exceeds a safe length. If using a server-side token, enforce expiry/ownership rules appropriate for a shareable public scenario and do not store private state in it.
7. Add copy-link and link-updated feedback. Include a reset/remove-share-state control.
8. Treat malformed, oversized, unsupported-version, and tampered state as recoverable input errors. Fall back to normal user defaults and show a useful message.

## Configure tab

1. Pre-check available owned/shared BPC options according to the existing availability rules. Do not pre-check unavailable copies.
2. Preserve the user’s explicit uncheck across tab changes, saves, reloads, and shared URLs where the choice is shareable. On a different user’s URL load, re-evaluate against that user’s own owned BPCs.
3. Default taxes to checked and load the tax calculation on initial page load for the Buy tab. Ensure this is idempotent and does not duplicate requests.
4. Remember the last system and structure for the user, while excluding them from shared URLs.
5. Add a source selector for BPC/BPO availability: all eligible locations, a selected station/structure, a selected hangar division, or a selected container/can where the ESI data supports it. Do not silently treat all owned assets across all locations as available when a narrower source is selected.
6. Add a matching materials source selector for a designated materials bay. Support structure/station, hangar division, and nested container/can where the asset payload can identify it. Save the selection as a user preference and expose it clearly in the Buy tab.
7. Structure names and options must be available from stored/cache data even when the current viewer lacks director rights. Store the structure ID and last known name when a properly authorized token has seen it. Use ESI to refresh names and rig data only when a token has the required scope and only when stale or explicitly requested. If the request fails, retain the cached name and omit unavailable rig updates.
8. Clearly label cached structure information and the last refresh time. Do not imply that cached data is live.
9. Keep all location and source selection server-validated. Never trust a location ID from a browser or shared URL as proof that the user can access it.

## Build tab and schedule tracking

### Layout and visualization

1. Reduce the visual height and density of each character slot card. Show the essential fields first: character, available slots, assigned jobs, and completion time. Put secondary skill/detail data behind a compact disclosure.
2. Restyle the schedule graph to use the existing theme variables, accessible contrast, restrained colors, and clear hover/focus states.
3. Make graph tooltips dynamic. A hover over a lane or time range must show the job or jobs occupying that interval, including item name, run range, start, end, duration, character, and whether it is estimated or live.
4. Do not assume the first item in a run represents the whole run. Return and render each scheduled chunk/job, or provide a grouped tooltip with all items in the hovered interval.
5. Make the graph usable with keyboard focus and provide a text/table fallback for screen readers and narrow screens.

### Opt-in live job tracking

Implement tracking only when both conditions are true: the user explicitly opts into tracking, and the user has calculated/saved a schedule slot plan.

1. Add a clear opt-in control and explain that tracking uses ESI industry-job data and may be stale or unavailable.
2. Let the user mark characters as working on this project. Store only the selected character references in private user/simulation state.
3. Persist the schedule snapshot used for tracking: item, run range, character, slot, planned start/end, expected duration, and calculation assumptions.
4. Match refreshed `IndustryJob` records conservatively by character/installer, blueprint or product, run range, station/structure where reliable, and time window. Never mark a job as matched solely because the item name is equal.
5. Add a service layer for tracking reconciliation so the view and task do not duplicate matching logic.
6. Refresh through the existing industry-job sync path when a valid token/scope is available. Do not poll ESI continuously from every browser tab. Use a bounded refresh interval, a task or endpoint with rate-limit handling, and a client countdown/refresh action.
7. Compute and display planned time versus actual observed time. Clearly distinguish running, completed, delayed, unmatched, unavailable, and stale states.
8. When a job completes, refresh dependent progress and material/output state without destroying user-entered price overrides or planner choices.
9. Add an audit-friendly timestamp for the last successful job refresh and the source of the status.
10. If ESI is unavailable or the token lacks scope, leave the schedule intact and explain that live tracking is temporarily unavailable.

### Recommendation quality

Replace vague schedule recommendations with actionable messages containing the reason and the expected effect, for example: “Move 2 runs from Character A to Character B; this removes 3h 20m from the critical path because Character B has an unused manufacturing slot.” Include assumptions and a link/focus target to the relevant lane or setting.

## Buy tab

### Guidance and terminology

1. Add helper text above the table explaining that `Real Price` is a user-entered or observed transaction price and `Estimate` is the fallback market/derived estimate. State which value calculations use.
2. Rename the Fuzzwork column and all controls to `Estimate`. Keep Fuzzwork in source metadata/tooltips where useful.
3. Add a short step guide above the shopping list: choose or enter owned materials, or designate a materials bay; review prices and taxes; click Re-calculate taxes if assumptions changed; click Compute shopping list; then export or copy leftovers.
4. Explain exactly what Export CSV exports and what Copy leftovers copies.
5. Rename ambiguous controls:
   - `Reset override` → `Clear real-price override`, with visible supporting text;
   - `Convert to compressed ore` → a label that states the action and target, such as `Use compressed ore equivalent`;
   - `Fuzzwork` → `Load estimates` or `Refresh estimates` depending on whether the action fetches or merely applies cached data;
   - `Calculate taxes` → `Re-calculate taxes`.
6. Move the shipping rate selector into the Shipping section and make its effect on totals clear.

### Correctness and visualization

1. Make owned versus needed quantities visually explicit with separate columns/badges for Needed, Owned, Buy, and Surplus. Use accessible color plus text/icons, not color alone.
2. Redesign the Items Needed badge so it is compact, aligned, readable at mobile widths, and consistent with the theme.
3. For a needed item available in buyback, add an accessible tooltip/popover explaining availability and the current displayed price. Include a button to start an order from that state.
4. Keep real price overrides in the canonical `SimulationAPI` price state and persist them when switching tabs, recalculating, saving, loading, and sharing where permitted. Do not reconstruct the Buy table from only server-rendered defaults.
5. Remove surplus value from net profit. Keep surplus as a separate informational value and document the profit formula.
6. For producible capital items without a market price, use the existing capital order estimate service/table. Put the result in the Estimate column with source metadata and mark it as an estimate. Do not overwrite a real price.
7. Add margin cells beside bought BPC rows so the table’s visual structure is consistent with produced rows. Define whether BPC margin is absolute, percentage, or not applicable; if not applicable, render a deliberate em dash with a tooltip.
8. Ensure taxes load only for the Buy tab on initial load, but remain available to every calculation that needs them after the first successful load.
9. Improve compressed ore calculations by using the project’s existing reprocessing logic and price source abstraction. If Janice is used, isolate it behind a service with timeout, cache, provenance, and Fuzzwork/known-data fallback. Never block the entire Buy tab on an external pricing service.

### Materials bay

1. Let the user designate a materials bay/can/hangar in Configure or Buy.
2. Save the selection per user and show the active source prominently.
3. On Compute, fetch or use cached assets from that source, subtract only eligible quantities, and show the refresh time.
4. Allow manual text-area input as an alternative. Explain that the user must choose exactly one source mode for a calculation.
5. Validate access and location ownership server-side. If asset data is stale or unavailable, require explicit confirmation before treating it as zero.

### Buyback order flow

When a needed item exists in buyback:

1. Offer `Create buyback order` from the row popover.
2. Open a modal containing item, quantity, displayed price, price source, available quantity, destination, and eligible character/recipient selection.
3. Require the user to confirm the quantity and recipient. Respect existing buyback permissions, validation, and order workflow.
4. Submit through the established material exchange order endpoint/service rather than duplicating order creation in the simulator.
5. On success, add the submitted quantity to the simulation’s owned-items view only as a clearly labeled pending/reserved quantity, then reconcile against authoritative stock/order data. Do not pretend the item is physically available before the order workflow says it is.
6. Handle partial availability, price changes, duplicate submissions, rejected orders, and expired buyback data.
7. Add tests for modal payload, permission denial, successful submission, and the pending-to-owned reconciliation.

## Plan tab

1. Move buying and building preferences to user-scoped defaults, while retaining per-simulation overrides in the saved snapshot.
2. Add quick instructions at the top of the tab. Explain what Optimize does, its objective, its inputs, and why the recommendation may change with skills, BPC coverage, prices, or slot choices.
3. Ensure profit margins use the same canonical real-price/estimate resolution as Buy. Changing tabs must not revert to stale server-rendered estimates.
4. Make material category counters self-explanatory or remove them.
5. Keep all calculations consistent when a simulation is loaded from a saved snapshot or a shared URL.

## Buy BPCs tab

1. Verify the tab against actual contract data and existing contract refresh code before changing the UI.
2. Display contract listed time, expiration time, last ESI refresh time, and local cache age. Use timezone-aware formatting and a full timestamp on hover/focus.
3. Clearly distinguish a current contract offer, a cached offer, an expired offer, and a missing/failed refresh.
4. Preserve selected contracts in simulation state, but revalidate them on load and before using them in cost/coverage calculations.
5. Add tests proving that contract metadata is rendered and that stale/expired data is not silently treated as current.

## ESI, cache, and failure policy

Use existing ESI clients, token scopes, cache models, and task patterns wherever possible.

- Check scope before requesting structures, assets, jobs, or contracts.
- Cache structure names and safe metadata so read access is not tied to director rights after the data has been observed.
- Refresh only stale records or when the user asks for a refresh.
- Preserve the last known value on transient failures.
- Expose freshness and failure state to the UI.
- Apply request timeouts, rate-limit handling, and bounded retries.
- Never let optional ESI enrichment erase valid saved simulation state.
- Log enough context to diagnose failures without logging tokens or private payloads.

## API and data-model guidance

Prefer small purpose-built endpoints/services over adding more responsibilities to the already large industry view.

Likely additions include:

- a user preference read/write endpoint;
- a share-state serializer/parser;
- an explicit simulation state save/load contract;
- a schedule tracking opt-in/character assignment endpoint;
- a schedule tracking refresh/reconciliation service;
- a materials-bay source endpoint;
- a BPC source endpoint;
- a buyback order draft/submit endpoint if the existing order API cannot accept the simulator payload;
- a capital price estimate adapter if the current service cannot provide provenance.

Every endpoint must enforce login, Indy Hub access, ownership, CSRF for browser mutations, input validation, and appropriate permission checks. Return stable JSON error shapes for JavaScript callers.

Use migrations for persistent fields. Add indexes for user/simulation lookups and tracking reconciliation fields only after confirming query patterns. Avoid storing large live ESI payloads in every simulation.

## Testing strategy

Add tests at the layer where behavior belongs.

### Server and model tests

- user preference defaults and isolation;
- per-user simulation-name uniqueness, including normalization and rename;
- complete simulation state save/load and old-state migration;
- shared-state allowlist and private-field exclusion;
- malformed/oversized/unsupported shared URL state;
- BPC/material source validation and permission checks;
- cached structures visible without director rights after authorized discovery;
- scope-aware ESI refresh and failure preservation;
- tax default/load behavior;
- capital price fallback and estimate provenance;
- schedule tracking opt-in and character assignment ownership;
- job matching, progress, planned-versus-actual duration, stale state, and ESI failure;
- buyback order permissions, quantity validation, price changes, partial availability, and pending ownership behavior;
- contract listed/expiration/cache timestamps;
- simulation endpoint access isolation.

### JavaScript and template tests

Where the repository’s current tooling permits, add focused tests for:

- canonical state round trips;
- real-price persistence across tab changes;
- URL serialization/deserialization;
- owned/needed/surplus calculations;
- tax initialization and re-calculation;
- dynamic schedule tooltip content;
- graph recommendation wording and focus behavior;
- control labels and export/copy payloads;
- buyback modal construction;
- graceful rendering when optional payload fields are absent.

If no browser test harness exists, keep logic in small pure functions and test them through the project’s available JavaScript test setup or document the manual browser checks precisely.

### Regression checks

Run the focused Django tests first, then the full project test command used by CI. Check the simulator manually in at least these states:

1. first-time user with no saved simulation;
2. user with owned BPCs and multiple characters;
3. user with no structure scope;
4. user with stale/failed ESI refresh;
5. saved simulation loaded after the underlying skills/materials/BPCs change;
6. another user opening a shared URL;
7. no prices, estimates only, real overrides, and capital estimate fallback;
8. taxes enabled/disabled and tab changes;
9. materials text input versus designated materials bay;
10. live tracking opted out, opted in with matching jobs, and opted in with unavailable ESI;
11. successful and rejected buyback order creation;
12. narrow/mobile layout and keyboard navigation for tooltips and graph details.

## Implementation order

Use small reviewable phases. Do not combine a large UI rewrite with an untested state migration.

### Phase 0: baseline and inventory

Document the current state flow, run focused tests, capture screenshots or manual notes for each tab, and record existing URL/local-storage compatibility requirements.

### Phase 1: information architecture and copy

Add discoverability, action-menu changes, clear tab subtitles, How To rewrite, control renames, removal of the ambiguous Back action, and reload explanation. This phase should be low risk and independently reviewable.

### Phase 2: canonical state and persistence

Implement the versioned state object, user preferences, saved-state completeness, unique names, tab persistence, tax defaults, real-price persistence, and migration handling.

### Phase 3: shareable URLs

Add the explicit allowlisted share schema, URL controls, validation, private-state exclusion, and cross-user behavior. Add tests before wiring every new UI control to the serializer.

### Phase 4: source-aware inventory and cached structures

Implement BPC source selection, materials-bay selection, cached structure visibility, scope-aware refresh, freshness indicators, and failure preservation.

### Phase 5: Buy tab correctness and presentation

Fix owned/needed visualization, estimates terminology, taxes, shipping placement, surplus profit treatment, capital estimates, compressed ore service, CSV/copy guidance, and real-price calculation consistency.

### Phase 6: Build schedule UX

Reduce character-card size, theme the graph, implement dynamic multi-item tooltips, improve recommendations, and add accessible text fallback.

### Phase 7: opt-in live tracking

Add schedule snapshotting, character opt-in, matching/reconciliation, bounded refresh, progress states, planned-versus-actual timing, and failure handling.

### Phase 8: Buy BPCs and buyback ordering

Add contract timestamps and freshness state, then implement the buyback popover/modal and pending ownership reconciliation through existing order infrastructure.

### Phase 9: remove or explain Run optimized

Make the retention decision from the documented behavior and tests. Remove dead code if retired; otherwise make the feature understandable and consistent with canonical state.

### Phase 10: polish, accessibility, documentation, and cleanup

Review translations, keyboard behavior, responsive layout, stale/dead JS, duplicate calculations, logs, migration safety, and all user-facing copy. Update How To with final names and behavior.

## Definition of done

The feature is complete when:

- a user can find the simulator from Industry without already knowing it is hidden in Blueprint Browser;
- first-use guidance explains every tab and the required order of operations;
- available BPCs and taxes have the requested defaults;
- user preferences persist without leaking into shared URLs;
- saved simulations restore the user’s exact workspace state while refreshing only live data;
- a shared URL reproduces safe scenario choices for another user using that user’s own private data;
- structure, asset, BPC, contract, and job data have explicit freshness and failure behavior;
- Buy calculations consistently distinguish real values from estimates and keep overrides across tabs;
- owned, needed, buy, surplus, taxes, shipping, profit, and margin values are visually and mathematically clear;
- Build schedules show every relevant item and provide useful, accessible recommendations;
- live job tracking is opt-in, bounded, auditable, and harmless when ESI is unavailable;
- buyback order creation is permission-safe and reconciles its pending state;
- saved simulation names are unique per user;
- the fate of Run optimized is documented and the UI contains no unexplained controls;
- focused tests and the full project test suite pass;
- the final Python code has been formatted with Black and checked with Ruff, with any intentional exclusions documented.

## Final validation and Black/Ruff pass

After implementation and tests, perform the following from the repository root using the project’s configured interpreter/environment:

1. Check the working tree and review the complete diff for accidental changes, debug output, secrets, generated files, and migration mistakes.
2. Run the focused simulator/material/industry tests.
3. Run the full project test command used by CI, preferably the supported tox environment or `runtests.py` command.
4. Run Black in check mode over changed Python files first, then the project’s intended Python package scope. If Black reports changes, apply them and rerun the affected tests.
5. Run Ruff over changed Python files and then the project scope. Fix all actionable findings rather than suppressing them broadly. Use narrowly scoped `# noqa` only when the codebase’s conventions and a documented reason justify it.
6. Run template/JavaScript/CSS checks that are already configured in `package.json`, pre-commit, or CI. At minimum perform syntax checks for changed JavaScript and inspect rendered template markup.
7. Rerun the focused tests after formatting/lint fixes, then rerun the full suite if Python behavior changed.
8. Record the exact commands and results in the implementation handoff, including any unavailable optional tool or known non-blocking warning.

The final handoff must list changed files, migrations, endpoint changes, user-visible behavior, test commands/results, Black result, Ruff result, and any intentionally deferred item.

