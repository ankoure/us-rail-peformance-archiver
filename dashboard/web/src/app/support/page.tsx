import type { Metadata } from "next";
import Link from "next/link";

import { feedCounts } from "@/lib/agencyParams";

/**
 * Static support page. The app is a static export (`output: "export"`), so there
 * is no server to create a checkout session, every option here is an outbound
 * link to a hosted funding page.
 *
 * Update these as the footprint changes; they're the whole argument for the page.
 */
const MONTHLY_COST_USD = 147;
const ARCHIVE_START = "May/June 2026";
const BILL_MONTH = "August 2026";

/**
 * Median fully-loaded cost of one agency: its own measured S3 usage plus a share
 * of the fixed infrastructure, weighted by poll volume. Recompute with
 * `scripts/s3_cost_report.py` when the fleet or the bill moves.
 *
 * Weight by poll volume, not feed count, feed count badly distorts agencies
 * with many barely-polled feeds (Bay Area 511 has 18 feeds but only 4 GiB
 * archived, and came out top of the list at $3.79 under per-feed weighting
 * while its real storage is $0.26).
 *
 * This is still an allocation, not a marginal cost, most of the bill is fixed,
 * so dropping a single agency would save far less than this. Tier copy says
 * "covers the cost of" for that reason, never "pays to add".
 */
const TYPICAL_AGENCY_USD = 0.65;

/** Suggested monthly amounts; mirror these as the GitHub Sponsors tiers. */
const TIERS = [
  { usd: 3, note: "Or most of MTA New York City Transit, the costliest single agency here." },
  { usd: 10, note: "A mid-size metro area's worth of agencies, indefinitely." },
  { usd: 25, note: "About a sixth of the entire bill." },
  { usd: 50, note: "A third of everything it takes to run this." },
];

const SPONSORS_URL = "https://github.com/sponsors/ankoure";
const KOFI_URL = "https://ko-fi.com/ankoure";

/**
 * Verified live 2026-09-03:
 *   gh api graphql -f query='{user(login:"ankoure"){hasSponsorsListing}}'  -> true
 *
 * Keep this a flag rather than deleting the branch: /sponsors/<user> redirects to
 * the profile instead of 404ing if a listing is ever withdrawn, so the CTA would
 * look fine and collect nothing.
 */
const SPONSORS_ENABLED = true;

const REPO_URL = "https://github.com/ankoure/us-rail-performance-archiver";

/**
 * Real line items from the AWS bill, rounded to the dollar. Route 53 is left out
 * because this project's DNS isn't hosted there, and part of the compute line is a
 * box shared with a sibling project so this is the archive's attributable share,
 * not the raw account total.
 */
const COSTS = [
  {
    what: "Compute",
    amount: 48,
    detail:
      "Regional poller hosts in the US, EU, and Australia, each polling every feed on its own interval, plus the host serving this dashboard's API.",
  },
  {
    what: "Storage",
    amount: 42,
    detail:
      "Every payload is landed raw before anything decodes it, so a parsing bug can never lose history. This line only grows archived bytes are never deleted, just aged into Deep Archive. The single largest holding is Minneapolis\u2013St Paul's Metro Transit, at over 100 GiB.",
  },
  {
    what: "Rollup compute",
    amount: 35,
    detail: "A scheduled Fargate task that turns the raw frames into queryable Parquet.",
  },
  {
    what: "Networking",
    amount: 10,
    detail: "Moving several hundred GB a month out of agency endpoints and into the archive.",
  },
  {
    what: "Everything else",
    amount: 12,
    detail: "Monitoring, alerting, and assorted small services.",
  },
];

export const metadata: Metadata = {
  title: "Support the Transit Dashboard",
  description: "Help cover the hosting costs of an open GTFS-Realtime archive.",
};

export default function SupportPage() {
  // Read at build time from config/feeds.yaml see feedCounts().
  const { agencies: agencyCount, feeds: feedCount } = feedCounts();
  const perAgencyUsd = MONTHLY_COST_USD / agencyCount;

  return (
    <main>
      <div className="card">
        <h2>Support the archive</h2>
        <p>
          Transit agencies publish GTFS-Realtime as a <em>live snapshot</em>, ask an agency what
          its trains were doing last Tuesday and, almost everywhere, the answer is that nobody kept
          it. This project keeps it: every poll from every configured feed, landed raw and rolled up
          into open Parquet, continuously since {ARCHIVE_START}.
        </p>
        <p>
          It runs on hardware I pay for out of pocket around{" "}
          <strong>${MONTHLY_COST_USD}/month</strong>. There is no company behind it and no grant
          funding it. If the bill stops getting paid, the archive doesn&rsquo;t just go offline; the
          history stops being recorded, and that gap can never be backfilled.
        </p>
        <div className="stat-row">
          <div className="stat-tile">
            <span className="value">{agencyCount}</span>
            <span className="label">agencies archived</span>
          </div>
          <div className="stat-tile">
            <span className="value">{feedCount}</span>
            <span className="label">feeds polled</span>
          </div>
          <div className="stat-tile">
            <span className="value">${perAgencyUsd.toFixed(2)}</span>
            <span className="label">per agency, per month</span>
          </div>
        </div>
        <p>
          That last number is the one worth sitting with: spread across everything it archives, this
          works out to about{" "}
          <strong>{Math.round(perAgencyUsd * 100)} cents per transit agency per month</strong> 
          less than a bus fare, for a permanent public record of how that agency actually ran.
        </p>
        <div className="support-actions">
          {SPONSORS_ENABLED && (
            <a
              className="support-button"
              href={SPONSORS_URL}
              target="_blank"
              rel="noopener noreferrer"
            >
              Sponsor on GitHub
            </a>
          )}
          <a
            className={
              SPONSORS_ENABLED ? "support-button support-button-secondary" : "support-button"
            }
            href={KOFI_URL}
            target="_blank"
            rel="noopener noreferrer"
          >
            {SPONSORS_ENABLED ? "One-time tip on Ko-fi" : "Support on Ko-fi"}
          </a>
        </div>
        <p className="card-hint">
          {SPONSORS_ENABLED
            ? "GitHub Sponsors takes no platform cut, so more of it reaches the bill. Ko-fi needs no account if you'd rather just drop something in once."
            : "Ko-fi takes both one-time and monthly support, and needs no account to give once. A zero-fee GitHub Sponsors option is on the way."}
        </p>
      </div>

      <div className="card">
        <h2>Where the money goes</h2>
        <ul className="support-list">
          {COSTS.map((c) => (
            <li key={c.what}>
              <div className="support-list-head">
                <span className="support-list-what">{c.what}</span>
                <span className="support-list-amount">${c.amount}/mo</span>
              </div>
              <span className="support-list-detail">{c.detail}</span>
            </li>
          ))}
        </ul>
        <p className="card-hint">
          Actual AWS line items for {BILL_MONTH}, rounded. Contributions cover infrastructure first.
          Anything past that goes toward onboarding more agencies every new feed is a permanent,
          recurring storage cost, so more funding genuinely means more of the world archived.
        </p>
      </div>

      <div className="card">
        <h2>What a monthly amount covers</h2>
        <ul className="support-tiers">
          {TIERS.map((t) => (
            <li key={t.usd}>
              <span className="support-tier-amount">${t.usd}/mo</span>
              <span className="support-tier-body">
                <span className="support-tier-what">
                  ~{Math.round(t.usd / TYPICAL_AGENCY_USD)} typical agencies
                </span>
                <span className="support-tier-note">{t.note}</span>
              </span>
            </li>
          ))}
        </ul>
        <p className="card-hint">
          A &ldquo;typical agency&rdquo; is the median one at ${TYPICAL_AGENCY_USD.toFixed(2)}/mo
          its own measured storage plus a share of the hosts, egress, and rollup compute, weighted
          by how hard it gets polled. Real agencies span $0.12 for a sleepy single-feed operator up
          to $4.33 for MTA New York City Transit, which runs eight separate subway-line feeds. These
          are shares of the total bill rather than the savings from dropping one agency, which would
          be far smaller: most of the cost is fixed no matter how many feeds ride on it.
        </p>
        <p className="card-hint">
          Monthly beats one-time here, as archived bytes are never deleted, so
          every agency is a bill that recurs forever. A recurring contribution is the only kind that
          matches the shape of the cost. Even a recurring donation of $1 would make an impact. 
        </p>
      </div>

      <div className="card">
        <h2>Other ways to help</h2>
        <p>
          Money isn&rsquo;t the only useful thing. Reporting a feed that&rsquo;s returning garbage,
          pointing me at an agency endpoint I don&rsquo;t have yet, or telling me a metric on this
          dashboard looks wrong are all worth real money in saved debugging time. Bonus points if 
          you&rsquo;re able to help get me an API for NJ Transit, I applied in March but haven&rsquo;t heard back. The{" "}
          <a className="support-inline-link" href={`${REPO_URL}/issues`}>
            issue tracker
          </a>{" "}
          is open, and so is the{" "}
          <a className="support-inline-link" href={REPO_URL}>
            source
          </a>
          .
        </p>
        <p className="card-hint">
          To be clear about what you&rsquo;re getting: this is a personal project, not a registered
          nonprofit, so contributions are a gift rather than a tax-deductible donation, and buy no
          influence over what gets archived. The code stays MIT-licensed and the dashboard stays
          free either way nothing here goes behind a paywall.{" "}
          <Link className="support-inline-link" href="/">
            Back to the agencies →
          </Link>
        </p>
      </div>
    </main>
  );
}
