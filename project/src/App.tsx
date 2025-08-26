import React, { useState, useEffect, useMemo } from 'react';
import Header from './components/Header';
import NewsGrid from './components/NewsGrid';
import FilterSidebar from './components/FilterSidebar';
import { Article, NewsFilters, Sourcename } from './types/news';
import { apiService } from './services/api';

function App() {
  const [articles, setArticles] = useState<Article[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>('');
  const [searchQuery, setSearchQuery] = useState('');
  const [sources, setSources] = useState<Sourcename[]>([]);
  const [featuredArticle, setFeaturedArticle] = useState<Article | null>(null);

  const [filters, setFilters] = useState<NewsFilters>({
    source: 'All',
    theme: 'All',
    dateRange: 'All Time',
    searchQuery: ''
  });

  // Load featured article
  useEffect(() => {
    const fetchFeatured = async () => {
      try {
        const response = await apiService.getFeaturedArticle();
        if (response.news && response.news.length > 0) {
          setFeaturedArticle(response.news[0]);
        }
      } catch (err) {
        console.error('Failed to load featured article:', err);
      }
    };
    fetchFeatured();
  }, []);

  // Load news and sources on mount
  useEffect(() => {
    const loadInitialData = async () => {
      setLoading(true);
      setError('');
      try {
        // Load sources
        const sourcesResp = await apiService.getSourcename();
        setSources(sourcesResp.Sourcename || []);

        // Load news
        let response;
        try {
          response = await apiService.getTopNews(500);
        } catch {
          response = await apiService.getRecentNews(500);
        }
        setArticles(response.news || response.results || []);
      } catch (err) {
        console.error('Failed to load initial data:', err);
        setError(err instanceof Error ? err.message : 'Failed to load initial data');
      } finally {
        setLoading(false);
      }
    };
    loadInitialData();
  }, []);

  // Handle search
  const handleSearch = async () => {
    if (!searchQuery.trim()) {
      // Reset to initial news
      let response;
      try {
        response = await apiService.getTopNews(500);
      } catch {
        response = await apiService.getRecentNews(500);
      }
      setArticles(response.news || response.results || []);
      return;
    }

    setLoading(true);
    setError('');
    try {
      const response = await apiService.searchNews(searchQuery, 20);
      setArticles(response.results || []);
      setFilters(prev => ({ ...prev, searchQuery }));
    } catch (err) {
      console.error('Search failed:', err);
      setError(err instanceof Error ? err.message : 'Search failed');
    } finally {
      setLoading(false);
    }
  };

  // Handle theme filter
  useEffect(() => {
    if (filters.theme !== 'All' && filters.theme !== '') {
      const loadFilteredNews = async () => {
        setLoading(true);
        setError('');
        try {
          const response = await apiService.filterNewsByTheme(filters.theme, 50);
          setArticles(response.news || []);
        } catch (err) {
          console.error('Failed to filter news:', err);
          setError(err instanceof Error ? err.message : 'Failed to filter news');
        } finally {
          setLoading(false);
        }
      };
      loadFilteredNews();
    }
  }, [filters.theme]);
const loadNews = async () => {
  setLoading(true);
  setError('');
  try {
    let response;
    try {
      response = await apiService.getTopNews(5000);
    } catch {
      response = await apiService.getRecentNews(5000);
    }
    setArticles(response.news || response.results || []);
  } catch (err) {
    console.error('Failed to load news:', err);
    setError(err instanceof Error ? err.message : 'Failed to load news');
  } finally {
    setLoading(false);
  }
};

  // Client-side filtering
  const filteredArticles = useMemo(() => {
    return articles.filter(article => {
      const matchesSource =
        filters.source === 'All' ||
        (article.source && article.source.toLowerCase().includes(filters.source.toLowerCase()));
      const matchesDateRange = filters.dateRange === 'All Time';
      return matchesSource && matchesDateRange;
    });
  }, [articles, filters.source, filters.dateRange]);

  const handleFiltersChange = (newFilters: NewsFilters) => {
    
    setFilters(newFilters);
    setFeaturedArticle(null);
    if (newFilters.searchQuery === '' && filters.searchQuery !== '') {
      setSearchQuery('');
      loadNews();
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <Header searchQuery={searchQuery} onSearchChange={setSearchQuery} onSearch={handleSearch} />

      <main className="max-w-65rem mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex flex-col lg:flex-row gap-8">
          <div className="flex-1">
            <NewsGrid
              featuredArticle={featuredArticle}
              articles={filteredArticles}
              loading={loading}
              error={error}
            />
          </div>
          <div className="lg:w-80">
            <FilterSidebar
              filters={filters}
              onFiltersChange={handleFiltersChange}
              articleCount={filteredArticles.length}
              sources={sources} // <-- pass sources here
            />
          </div>
        </div>
      </main>

      <footer className="bg-white border-t border-gray-200 mt-16">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="text-center text-gray-500 text-sm">
            © 2025 NewsPortal. Connected to GDELT database via Flask API.
            <br />
            Click on articles to visit original news sources.
          </div>
        </div>
      </footer>
    </div>
  );
}

export default App;
