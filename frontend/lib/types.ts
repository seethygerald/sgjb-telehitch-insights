export type RouteTab = "within-sg" | "sg-jb";

export type TelehitchRequest = {
  gold_request_id: string;
  silver_request_id: string | null;
  channel: string | null;
  topic_id: string | null;
  message_id: string | null;
  message_date_gmt8: string;
  scraped_at_gmt8: string | null;
  sender_id: string | null;
  sender_handle: string | null;
  request_type: string | null;
  request_time_text: string | null;
  pax_count: number | null;
  pickup_location: string | null;
  dropoff_location: string | null;
  normalized_pickup_location: string | null;
  normalized_dropoff_location: string | null;
  pickup_postal_code: string | null;
  pickup_latitude: number;
  pickup_longitude: number;
  pickup_formatted_address: string | null;
  pickup_resolution_source: string | null;
  dropoff_postal_code: string | null;
  dropoff_latitude: number;
  dropoff_longitude: number;
  dropoff_formatted_address: string | null;
  dropoff_resolution_source: string | null;
  message: string | null;
};

export type RequestsResponse = {
  generated_at: string;
  minutes: number;
  tab: RouteTab;
  count: number;
  tracked_count: number;
  requests: TelehitchRequest[];
};

export type DatabricksStatementResponse = {
  statement_id: string;
  status: { state: "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELED" | "CLOSED"; error?: { message?: string } };
  manifest?: { schema?: { columns?: Array<{ name: string; type_name?: string; type_text?: string }> } };
  result?: { data_array?: unknown[][] };
};
