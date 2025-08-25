import { Article, ApiResponse, Theme, Stats, Sourcename } from '../types/news';

const API_BASE_URL = 'http://localhost:8000/api';

class ApiService {
  private async fetchWithErrorHandling<T>(url: string): Promise<T> {
    try {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return await response.json();
    } catch (error) {
      console.error('API request failed:', error);
      throw error;
    }
  }

  async getRecentNews(limit: number = 500): Promise<ApiResponse<Article>> {
    return this.fetchWithErrorHandling(`${API_BASE_URL}/news/recent?limit=${limit}`);
  }

  async getFeaturedArticle(): Promise<ApiResponse<Article>> {
    // Cast the response to the expected type with raw data
    const response = await this.fetchWithErrorHandling<ApiResponse<any>>(`${API_BASE_URL}/featured?limit=1`);
    
    // Create a properly typed response object
    const typedResponse: ApiResponse<Article> = {
      ...response,
      news: response.news ? response.news.map((item: any): Article => ({
        id: item.id,
        title: item.title,
        actor1_name: item.actor1_name,
        actor2_name: item.actor2_name,
        event_date: item.event_date,
        num_mentions: item.nummentions,          // normalized
        avg_tone: item.avgtone,
        confidence: item.confidence,
        source: item.sourcecommonname,           // normalized
        source_url: item.url,
        document_url: item.documentidentifier,
        image: item.image,
        theme: item.first_theme,                 // optional
      })) : undefined
    };

    return typedResponse;
  }

  async getLatestNews(hours: number = 24, limit: number = 20): Promise<ApiResponse<Article>> {
    return this.fetchWithErrorHandling(`${API_BASE_URL}/news/latest?hours=${hours}&limit=${limit}`);
  }

  async getTopNews(limit: number = 5000): Promise<ApiResponse<Article>> {
    return this.fetchWithErrorHandling(`${API_BASE_URL}/news/top?limit=${limit}`);
  }

  async searchNews(query: string, limit: number = 20): Promise<ApiResponse<Article>> {
    const encodedQuery = encodeURIComponent(query);
    return this.fetchWithErrorHandling(`${API_BASE_URL}/search?q=${encodedQuery}&limit=${limit}`);
  }

  async filterNewsByTheme(theme: string, limit: number = 50): Promise<ApiResponse<Article>> {
    const encodedTheme = encodeURIComponent(theme);
    return this.fetchWithErrorHandling(`${API_BASE_URL}/news/filter?theme=${encodedTheme}&limit=${limit}`);
  }

  async getNewsDetail(newsId: string): Promise<{ news: Article }> {
    return this.fetchWithErrorHandling(`${API_BASE_URL}/news/${newsId}`);
  }

  async getThemes(): Promise<{ themes: Theme[] }> {
    return this.fetchWithErrorHandling(`${API_BASE_URL}/themes`);
  }

  async getSourcename(): Promise<{ Sourcename: Sourcename[] }> {
    return this.fetchWithErrorHandling(`${API_BASE_URL}/sourcename`);
  }
  async getStats(): Promise<{ stats: Stats }> {
    return this.fetchWithErrorHandling(`${API_BASE_URL}/stats`);
  }

  async healthCheck(): Promise<{ status: string; timestamp: string }> {
    return this.fetchWithErrorHandling(`${API_BASE_URL}/health`);
  }
}

export const apiService = new ApiService();