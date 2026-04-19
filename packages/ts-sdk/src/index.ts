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
        throw new Error(`request failed: ${response.status}`);
      }

      return response.json();
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
        throw new Error(`request failed: ${response.status}`);
      }

      return response.json();
    }
  };
}
