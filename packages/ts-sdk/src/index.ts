export type Placement = "home" | "search" | "detail" | "mypage";

export type ContentKind = "school" | "event" | "article";

export type RecommendationAreaContext = {
  country?: string | null;
  prefecture_code?: string | null;
  prefecture_name?: string | null;
  city_code?: string | null;
  city_name?: string | null;
};

export type RecommendationContextInput = {
  station_id?: string | null;
  line_id?: string | null;
  line_name?: string | null;
  area?: RecommendationAreaContext | null;
};

export type RecommendationRequest = {
  request_id?: string | null;
  target_station_id?: string | null;
  context?: RecommendationContextInput | null;
  limit?: number | null;
  user_id?: string | null;
  placement?: Placement;
  debug?: boolean;
};

export type ScoreComponent = {
  feature: string;
  value: number;
  reason: string;
  details?: Record<string, unknown> | null;
};

export type RecommendationItem = {
  content_kind: ContentKind;
  content_id: string;
  school_id: string;
  school_name: string;
  event_id?: string | null;
  event_title?: string | null;
  primary_station_id: string;
  primary_station_name: string;
  line_name: string;
  score: number;
  explanation: string;
  score_breakdown: ScoreComponent[];
  fallback_stage?:
    | "strict_station"
    | "same_line"
    | "same_city"
    | "same_prefecture"
    | "neighbor_area"
    | "safe_global_popular"
    | null;
};

export type RecommendationResponse = {
  request_id?: string | null;
  items: RecommendationItem[];
  explanation: string;
  score_breakdown: ScoreComponent[];
  fallback_stage:
    | "strict_station"
    | "same_line"
    | "same_city"
    | "same_prefecture"
    | "neighbor_area"
    | "safe_global_popular";
  candidate_counts: Record<string, number>;
  context?: {
    context_source:
      | "request_station"
      | "request_line"
      | "request_area"
      | "user_profile_area"
      | "recent_search_context"
      | "recent_behavior_context"
      | "default_safe_context";
    confidence: number;
    privacy_level: "coarse_area";
    warnings: Array<{ code: string; message: string }>;
  } | null;
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
  idempotency_key?: string | null;
  user_id: string;
  event_kind: TrackEventKind;
  school_id?: string | null;
  event_id?: string | null;
  target_station_id?: string | null;
  context?: RecommendationContextInput | null;
  occurred_at?: string | null;
  payload?: Record<string, unknown> | null;
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
