export interface Article {
  id: string;
  title: string;
  actor1_name?: string;
  actor2_name?: string;
  event_date?: string;
  num_mentions: number;
  avg_tone?: number;
  confidence?: number;
  source_url?: string;
  source: string;
  theme?: string;
  image?: string;
  similarity_score?: number;
  themes?: string;
  persons?: string;
  organizations?: string;
  goldstein_scale?: number;
  document_url?: string;
  avg_confidence?: number;
}

export interface NewsFilters {
  source: string;
  theme: string;
  dateRange: string;
  searchQuery: string;
}

export interface ApiResponse<T> {
  news?: T[];
  results?: T[];
  total: number;
  error?: string;
  query?: string;
}

export interface Theme {
  theme: string;
  count: number;
}

export interface Stats {
  total_events: number;
  total_mentions: number;
  total_gkg: number;
  total_themes: number;
  recent_events: number;
}

export interface Sourcename {
  count: number;
  source: string;
  
}