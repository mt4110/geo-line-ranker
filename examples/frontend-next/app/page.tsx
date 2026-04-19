"use client";

import { useMemo, useState } from "react";

type ScoreComponent = {
  feature: string;
  value: number;
  reason: string;
};

type RecommendationItem = {
  content_kind: "school" | "event" | "article";
  content_id: string;
  school_id: string;
  school_name: string;
  event_id?: string;
  event_title?: string;
  primary_station_id: string;
  primary_station_name: string;
  line_name: string;
  score: number;
  explanation: string;
  score_breakdown: ScoreComponent[];
};

type RecommendationResponse = {
  items: RecommendationItem[];
  explanation: string;
  score_breakdown: ScoreComponent[];
  fallback_stage: "strict" | "neighbor";
  profile_version: string;
  algorithm_version: string;
};

const stationOptions = [
  { value: "st_tamachi", label: "Tamachi" },
  { value: "st_shinbashi", label: "Shinbashi" },
  { value: "st_hamamatsucho", label: "Hamamatsucho" },
  { value: "st_shibuya", label: "Shibuya" }
];

const placementOptions = [
  { value: "home", label: "Home" },
  { value: "search", label: "Search" },
  { value: "detail", label: "Detail" },
  { value: "mypage", label: "Mypage" }
] as const;

export default function Page() {
  const [targetStationId, setTargetStationId] = useState("st_tamachi");
  const [placement, setPlacement] =
    useState<(typeof placementOptions)[number]["value"]>("home");
  const [limit, setLimit] = useState(3);
  const [response, setResponse] = useState<RecommendationResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const apiBaseUrl = useMemo(
    () => process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:4000",
    []
  );

  async function submitForm(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsLoading(true);
    setError(null);

    try {
      const result = await fetch(`${apiBaseUrl}/v1/recommendations`, {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify({
          target_station_id: targetStationId,
          placement,
          limit
        })
      });

      if (!result.ok) {
        throw new Error(`API request failed with ${result.status}`);
      }

      const payload = (await result.json()) as RecommendationResponse;
      setResponse(payload);
    } catch (submitError) {
      setError(
        submitError instanceof Error ? submitError.message : "Unknown request error"
      );
      setResponse(null);
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <main className="shell">
      <section className="workspace">
        <div className="toolPanel">
          <div className="toolHeader">
            <p className="eyebrow">Phase 6 Example</p>
            <h1>Placement-aware ranking</h1>
            <p className="lede">
              Mixed school and event ranking with placement profiles and diversity control.
            </p>
          </div>

          <form className="requestForm" onSubmit={submitForm}>
            <label>
              Target station
              <select
                value={targetStationId}
                onChange={(event) => setTargetStationId(event.target.value)}
              >
                {stationOptions.map((station) => (
                  <option key={station.value} value={station.value}>
                    {station.label}
                  </option>
                ))}
              </select>
            </label>

            <label>
              Placement
              <select
                value={placement}
                onChange={(event) =>
                  setPlacement(event.target.value as (typeof placementOptions)[number]["value"])
                }
              >
                {placementOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>

            <label>
              Result count
              <input
                min={1}
                max={5}
                type="number"
                value={limit}
                onChange={(event) => setLimit(Number(event.target.value))}
              />
            </label>

            <button disabled={isLoading} type="submit">
              {isLoading ? "Ranking..." : "Run recommendation"}
            </button>
          </form>
        </div>

        <div className="visualPanel">
          <img
            alt="Rail platform for the example frontend"
            className="visualImage"
            src="https://images.unsplash.com/photo-1474487548417-781cb71495f3?auto=format&fit=crop&w=1200&q=80"
          />
          <div className="visualMeta">
            <span>{apiBaseUrl}</span>
            <span>{response?.profile_version.slice(0, 12) ?? "phase6-profile"}</span>
          </div>
        </div>
      </section>

      <section className="results">
        <div className="resultsHeader">
          <h2>Response viewer</h2>
          <p>
            {response
              ? `${placement} / ${response.fallback_stage}`
              : "Run the request to compare placement behavior and diversity."}
          </p>
        </div>

        {error ? <p className="errorText">{error}</p> : null}

        {response ? (
          <>
            <div className="summaryLine">
              <strong>{response.explanation}</strong>
            </div>
            <div className="itemList">
              {response.items.map((item) => (
                <article className="itemRow" key={item.content_id}>
                  <div>
                    <div className="itemHeading">
                      <p className="itemName">
                        {item.content_kind === "event"
                          ? item.event_title ?? item.school_name
                          : item.school_name}
                      </p>
                      <span className="kindBadge">{item.content_kind}</span>
                    </div>
                    <p className="itemMeta">
                      {item.school_name} / {item.primary_station_name} / {item.line_name}
                    </p>
                  </div>
                  <div className="itemScore">{item.score.toFixed(2)}</div>
                  <p className="itemExplanation">{item.explanation}</p>
                  <ul className="breakdownList">
                    {item.score_breakdown.map((component) => (
                      <li key={`${item.content_id}-${component.feature}`}>
                        <span>{component.feature}</span>
                        <strong>{component.value.toFixed(2)}</strong>
                        <p>{component.reason}</p>
                      </li>
                    ))}
                  </ul>
                </article>
              ))}
            </div>
          </>
        ) : (
          <p className="emptyText">
            Default seed data includes mixed school and event candidates around Tamachi,
            Shinbashi, and Shibuya.
          </p>
        )}
      </section>
    </main>
  );
}
