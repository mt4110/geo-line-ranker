export type Placement = "home" | "search" | "detail" | "mypage";

export type ContentKind = "school" | "event" | "article";

export type RecommendationRequest = {
  target_station_id: string;
  limit?: number;
  user_id?: string;
  placement?: Placement;
  debug?: boolean;
};

export type ScoreComponent = {
  feature: string;
  value: number;
  reason: string;
  details?: Record<string, unknown>;
};

export type RecommendationItem = {
  content_kind: ContentKind;
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

export type RecommendationResponse = {
  items: RecommendationItem[];
  explanation: string;
  score_breakdown: ScoreComponent[];
  fallback_stage: "strict" | "neighbor";
  profile_version: string;
  algorithm_version: string;
};

export type TrackEventKind =
  | "school_view"
  | "school_save"
  | "search_execute"
  | "event_view"
  | "apply_click"
  | "share";

export type TrackRequest = {
  user_id: string;
  event_kind: TrackEventKind;
  school_id?: string;
  event_id?: string;
  target_station_id?: string;
  occurred_at?: string;
  payload?: Record<string, unknown>;
};

export type TrackResponse = {
  status: string;
  event_id: string;
  queued_jobs: string[];
};

async function buildRequestError(response: Response): Promise<Error> {
  const contentType = response.headers.get("content-type") ?? "";
  let detail = "";

  try {
    if (contentType.includes("application/json")) {
      const payload = (await response.json()) as unknown;
      detail = JSON.stringify(payload);
    } else {
      detail = await response.text();
    }
  } catch {
    detail = "";
  }

  const boundedDetail = detail.trim().slice(0, 512);
  const suffix = boundedDetail ? ` - ${boundedDetail}` : "";
  return new Error(
    `request failed: ${response.status} ${response.statusText}${suffix}`.trim()
  );
}

export function createClient(baseUrl: string) {
  const apiBaseUrl = baseUrl.replace(/\/+$/, "");

  return {
    async recommend(input: RecommendationRequest): Promise<RecommendationResponse> {
      const response = await fetch(`${apiBaseUrl}/v1/recommendations`, {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(input)
      });

      if (!response.ok) {
        throw await buildRequestError(response);
      }

      return (await response.json()) as RecommendationResponse;
    },

    async track(input: TrackRequest): Promise<TrackResponse> {
      const response = await fetch(`${apiBaseUrl}/v1/track`, {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(input)
      });

      if (!response.ok) {
        throw await buildRequestError(response);
      }

      return (await response.json()) as TrackResponse;
    }
  };
}
