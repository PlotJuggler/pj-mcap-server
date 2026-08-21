# Business positioning: the niche, the competition, the shape (2026-08-10)

**Status:** strategy record — market research plus a positioning argument, written for the
question *"if we wanted to make this a small but sustainable business, are we doing
anything different and valuable enough to hold a niche?"* Fifth of the companion records
(alternatives review · capability review · rewrite note · greenfield architecture · this).

Answer up front: **yes — but the wedge is sovereignty and native multimodal experience,
not price**, and the single biggest asset is not in the codebase at all.

---

## 1. The competitive map (August 2026)

| Player | Model | Who they chase |
|---|---|---|
| **Foxglove** | SaaS; free tier (3 seats / 10 GB); Pro **$20/mo with 1 TB + unlimited viewer seats** after a large 2026 price cut; enterprise BYOS self-hosted lake; unified data search/curation platform launched April 2026 | Everyone; enterprise sales motion for self-hosted |
| **Rerun** | OSS core + Rerun Hub (commercial, preview); Rust; `.rrd`-centric; dataloader-from-recordings for training | Physical-AI / robot-learning labs, petabyte scale |
| **Nominal** (~$102.5M raised) / **Sift** | Enterprise observability for hardware test & telemetry | Aerospace, defense, government |
| **Roboto** | SaaS analytics engine (index + topic stats + RoboQL), seed-stage | Ingest-and-analyze teams |
| **Heex** | Enterprise "smart data" — event-triggered capture | ADAS, large fleets |
| **coScene** | SceneOps data platform | Strong in Asia |
| **ReductStore** | Open-core (core Apache 2.0 since v1.19); Pro ≈ $150–300/mo; edge-first store + ROS agent + conditional replication | **Complementary, not competing**: candidate edge/ingestion tier upstream of the lake (integration assessed in the architecture note §2.9) |
| **Lichtblick** (BMW's fork of Foxglove Studio) | Free OSS, visualization only | The community that left Foxglove |

Nobody on this list sells a **self-hosted, bring-your-own-bucket MCAP catalog + subset-
streaming server with a native desktop client and no vendor in the data path**. The two
closest gestures at it — Foxglove BYOS/index-in-place and ReductStore Pro — are
respectively enterprise-sales-gated with a managed control plane, and a record-grain
edge store that never looks inside an MCAP.

## 2. Demand signals for exactly this posture

Three independent signals, one of them a wound:

1. **Foxglove built index-in-place and BYOS at all.** They ran the opposite architecture
   (transcode into their lake) for years, then added "leave the files in the customer's
   bucket, only take notes" — because teams that treat their own bucket as the permanent
   archive kept demanding it. The market leader validated the posture this project
   started with.
2. **The Foxglove Studio license closure (2023) left institutional distrust.** The
   community reaction was fierce, and BMW made the distrust permanent by maintaining
   Lichtblick as a free fork. There is a real constituency — including large industrial
   players — that will not build on Foxglove again and currently has visualization
   without a data platform.
3. **Every funded competitor is running up-market** — petabyte physical-AI (Rerun),
   enterprise defense (Nominal, Sift), large ADAS fleets (Heex). The 5–50-person robotics
   company with a growing MCAP pile and no data team is being abandoned by the money,
   which is precisely the shelter a small sustainable business wants.

## 3. What is genuinely different here, ranked by defensibility

1. **PlotJuggler distribution and the author's name.** The decisive asset, and it is not
   in this repository. Tens of thousands of engineers open PlotJuggler daily; the
   connector puts the product inside a tool they already trust, from the author they
   already trust — zero customer-acquisition cost, aimed at exactly the community
   Foxglove burned. No competitor can replicate this; they would have to buy it.
2. **Native, multimodal, desktop-grade experience against a cloud archive.**
   PlotJuggler 4 is fully multimodal — time series, 3D scene, images — at native
   performance (this repo's own harness streams a `/tf` + pointcloud recording into the
   3D scene). So the pitch is not "a plotting niche beside the web platforms" but **the
   native, sovereign alternative to them**: everything the web platforms do in a browser
   tab, done natively, against the customer's own bucket. Foxglove is web-first by
   identity — following here would mean abandoning their platform thesis. Structural
   moat, not feature gap. Multimodal also raises the value of what this design is
   uniquely good at: topic-subset stitched streaming, the high-bitrate-separation
   producer rule, and the preview-projection tier all exist to make heavy modalities
   feel instant over a WAN.
3. **Genuinely sovereign self-hosting.** Two binaries plus the customer's bucket;
   air-gap capable; no managed control plane; no meter. For defense-adjacent, medical,
   EU-sovereignty, and factory-floor buyers, "no vendor in the data path, ever" is a
   requirement — and it is structurally incompatible with how funded SaaS makes money.
   This is what makes the niche *defensible* rather than merely unoccupied.
4. **MCAP-native, zero conversion, zero duplication + the open-formats analytical door**
   (planned). Index-in-place itself is now table stakes (Foxglove has it); combined with
   catalog-as-open-tables it becomes the checkable version of "no lock-in".

**Not differentiated, and deliberately not contested:** ML training pipelines (Rerun's
up-market fight; the LeRobot-export projection covers the pragmatic need without
competing for it), event-detection breadth, funded-SaaS polish. A small business dies
the day it feature-races a funded roadmap.

**The one wedge that no longer works: price.** At $20/month with unlimited viewer
seats, Foxglove's SaaS cannot be undercut meaningfully. The wedge is what they won't
do (sovereignty, native), not what they charge.

## 4. The niche, named

> **The sovereign, native, multimodal data platform for robotics teams — from the
> PlotJuggler author, on your own bucket.**

Customers, concretely: the small-to-mid robotics company (5–50 engineers) with a growing
MCAP lake and no data team; defense/medical/industrial teams that cannot SaaS; EU
data-sovereignty buyers; and the existing PlotJuggler user base as the standing funnel.

## 5. Business shape that fits "small but sustainable"

- **Open-core.** Free connector + free single-user server: grows the funnel and the
  community, and keeps faith with the open-source audience that is the moat.
- **Flat per-deployment annual license** for team features — multi-user auth, tag/event
  editing, analyzers, the analytical door — at self-serve prices (order of a few
  k€/site/year), explicitly *not* metered per GB/seat: predictability is part of the
  product for this buyer.
- **Support contracts** for the regulated/air-gapped buyers, who expect to pay for them.
- **Sponsored-feature development** — which is literally what this project already is: a
  customer funding a self-hosted tool. The revenue model has a working prototype, not
  just the product.

## 6. Risks, stated plainly

- **The niche is small.** That matches the stated ambition (sustainable, not venture-
  scale); it does not support a big team.
- **Foxglove could ship a truly self-serve self-hosted tier.** Their incentives point
  away from it (it cannibalizes the meter), but it is the move to watch.
- **The moat is a person.** The distribution asset and the trust asset are the author's;
  the bus factor is the moat and the risk at once.
- **A second widening move (added 2026-08-21):** ReductStore integration as the ingestion
  funnel — their edge store feeding this lakehouse via an unloader is complementary by
  construction (they stop where we start), both sides are open-source-aligned, and a
  documented integration converts their robot-side users into lake-side prospects.
- **One cheap widening move:** the wire protocol is open protobuf — a Lichtblick client
  for it would turn the BMW-fork community from bystanders into a second funnel, and
  make the platform multi-client by demonstration rather than promise.

---

## 7. The wedge decision: diagnostics first, ML as a projection (2026-08-21)

**The question:** focus the product on the ML training-data use case (where Rerun is
aiming), on fleet diagnostics over recorded data, or both — and is this an early
commitment or an easy pivot?

**The decision: fleet diagnostics is the wedge. The ML export ships as a feature, not
as the pitch.** "Both" lives in the architecture (which is use-case-agnostic by
design); "diagnostics" lives in every sentence a customer reads. What a wedge choice
actually fixes early is persona, messaging, and roadmap order — not code.

### 7.1 Market evidence (researched 2026-08-21)

**The ML side is a capital war converging on a free floor.** Rerun: ~$23.2M raised,
Rerun Hub explicitly "the data layer for Physical AI" (catalog + storage engine +
GPU-feeding dataloaders; early-availability, contact-sales). Foxglove's April 2026
relaunch is framed entirely as physical-AI curation. Lightwheel: ~$100M Q1-2026 orders
on $145M raised (sim/synthetic). Encord: $60M Series C, physical-AI revenue up 10x.
Beneath them, Hugging Face's LeRobot ecosystem passed **58,000 community datasets**
(50x YoY) with free curation tooling. Collection budgets are real but concentrated in
funded labs (a 2,000-demo bimanual IL dataset runs ~$110–215k). **No evidence was
found that ordinary non-foundation-model robotics SMEs pay for ML data tooling at
all** — the free LeRobot stack plus VC-subsidized platforms covers them.

**The diagnostics side has loud, organic, unmonetized pain.** An Oct 2025 HN thread on
robotics data management: "combing through the syslogs to find issues is an absolute
nightmare… even more so if you are told the machine broke at some point last night";
silent failures found long after the fact; everyone on custom scripts. ROS Discourse
keeps sprouting DIY triage tools (e.g. `robot-triage`, Aug 2026). Companies post
$180–215k data-infrastructure roles to build this in-house — the pricing anchor for a
tool far below one engineer-year. Competitively: live-telemetry players (Formant
~$250–600/robot/mo; InOrbit) do real-time dashboards, not archive forensics. Roboto
($4.8M seed, hosted SaaS) is the one pure recorded-log play — and its **Roboto Agents**
announcement (2026-08-17: agentic triage + root-cause over rosbags/MCAP) shows where
the feature bar is going. Foxglove BYOS keeps data in the customer's bucket but the
control plane and index compute remain Foxglove's, enterprise-gated. **Nobody sells
post-hoc fleet diagnostics over an MCAP archive the customer owns, fully self-hosted,
priced for a 5–50-person team.** That is this project's exact posture (§4).

### 7.2 The pivot-cost asymmetry — why the decision is easier than it looks

Given the lakehouse architecture (immutable lake + artifact registry + projections),
the two pivot directions are not symmetric:

- **Diagnostics-first → add ML later:** the LeRobot export is one Tier-3 copyist
  policy in the registry — a projection. Weeks of work, already designed (architecture
  note; "archives preserve, exports opine"). It becomes a checkbox ("your archive is
  training-ready") and an expansion sale to an existing customer.
- **ML-first → add diagnostics later:** requires building the entire interactive
  surface — browse, facets, event search, incident workflow, the streaming door — for
  a persona not yet reached, while re-earning distribution among the engineers skipped
  the first time. A second company, not a feature release.

When pivot costs are this asymmetric, wedge on the side that keeps the other option
cheap. Three more forces point the same way: **(1) distribution** — PlotJuggler's
daily users are diagnosticians; the funnel converts to a diagnostics product at zero
acquisition cost and to an ML-curation product barely at all; **(2) sovereignty
sells to the diagnostics buyer** — the cannot-SaaS segment has diagnostics pain today
and ML ambitions someday, while physical-AI labs are cloud-native and VC-subsidized;
**(3) land-and-expand runs downhill** — the fleet whose recordings we catalog for
diagnostics is the fleet that will eventually train policies; being the system of
record means the ML expansion comes to us, whereas Rerun must fight *down* into
fleets to get what we would already hold.

### 7.3 Consequences and revisit triggers

- Analyzer roadmap orders by triage value (bump/battery/error-burst detection), not
  dataset curation; the long-stubbed automatic tagging is, in this light, the product
  roadmap — and the substrate an agentic-triage layer (the Roboto Agents feature bar)
  plugs into.
- The wedge must be *genuine* self-hosting (air-gap, no egress, no vendor control
  plane) plus the PJ-native workflow — Foxglove's $20/mo Pro + BYOS is "close enough"
  for teams without hard sovereignty requirements, so do not feature-race their
  roadmap.
- **Revisit trigger:** a real customer with a real budget pulling toward training
  data. That is handled as an expansion (ship the export projection), not a pivot.

---

*Market facts referenced: Foxglove reduced self-service pricing and basic-seats
announcements, BYOS and index-in-place launches, April 2026 search/curation platform
launch; Nominal/Sift funding via CB Insights; ReductStore pricing via Capterra; the
2023 Foxglove Studio license-closure reaction and the Lichtblick fork. Gathered
2026-08-10; re-verify before acting on any of them.*
