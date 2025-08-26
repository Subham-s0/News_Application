import React, { useState, useEffect, useMemo, useCallback } from 'react';
import Header from './components/Header';
import NewsGrid from './components/NewsGrid';
import FilterSidebar from './components/FilterSidebar';
import { Article, NewsFilters, Sourcename } from './types/news';
import { apiService } from './services/api';

// Debounce hook for search optimization
const useDebounce = (value: string, delay: number): string => {
  const [debouncedValue, setDebouncedValue] = useState<string>(value);

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedValue(value);
    }, delay);

    return () => {
      clearTimeout(handler);
    };
  }, [value, delay]);

  return debouncedValue;
};

function App(): JSX.Element {
  const [articles, setArticles] = useState<Article[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>('');
  const [searchQuery, setSearchQuery] = useState('');
  const [sources, setSources] = useState<Sourcename[]>([]);
  const [featuredArticle, setFeaturedArticle] = useState<Article | null>(null);
  const [allArticles, setAllArticles] = useState<Article[]>([]); // Store all articles for fallback

  const [filters, setFilters] = useState<NewsFilters>({
    source: 'All',
    theme: 'All',
    dateRange: 'All Time',
    searchQuery: ''
  });

  // Debounce search query to avoid too many API calls
  const debouncedSearchQuery = useDebounce(searchQuery, 500);

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

  // Load initial data
  useEffect(() => {
    const loadInitialData = async () => {
      setLoading(true);
      setError('');
      try {
        // Load sources
        const sourcesResp = await apiService.getSourcename();
        setSources(sourcesResp.Sourcename || []);

        // Load initial news
        let response;
        try {
          response = await apiService.getTopNews(500);
        } catch {
          response = await apiService.getRecentNews(500);
        }
        
        const initialArticles = response.news || response.results || [];
        setArticles(initialArticles);
        setAllArticles(initialArticles); // Store for fallback
      } catch (err) {
        console.error('Failed to load initial data:', err);
        setError(err instanceof Error ? err.message : 'Failed to load initial data');
      } finally {
        setLoading(false);
      }
    };
    loadInitialData();
  }, []);

  // Handle real-time search
  useEffect(() => {
    const performSearch = async () => {
      if (!debouncedSearchQuery.trim()) {
        // If search is empty, restore original articles
        setArticles(allArticles);
        setFilters(prev => ({ ...prev, searchQuery: '' }));
        return;
      }

      setLoading(true);
      setError('');
      try {
        const response = await apiService.searchNews(debouncedSearchQuery);
        setArticles(response.news || []);
        setFilters(prev => ({ ...prev, searchQuery: debouncedSearchQuery }));
        setFeaturedArticle(null); // Hide featured article during search
      } catch (err) {
        console.error('Search failed:', err);
        setError(err instanceof Error ? err.message : 'Search failed');
        // Fallback to client-side filtering if API search fails
        const filteredArticles = allArticles.filter(article =>
          article.title?.toLowerCase().includes(debouncedSearchQuery.toLowerCase()) ||
          article.actor1_name?.toLowerCase().includes(debouncedSearchQuery.toLowerCase()) ||
          article.actor2_name?.toLowerCase().includes(debouncedSearchQuery.toLowerCase()) ||
          article.theme?.toLowerCase().includes(debouncedSearchQuery.toLowerCase())
        );
        setArticles(filteredArticles);
      } finally {
        setLoading(false);
      }
    };

    performSearch();
  }, [debouncedSearchQuery, allArticles]);

  // Handle theme filter
  useEffect(() => {
    if (filters.theme !== 'All' && filters.theme !== '' && !searchQuery) {
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
  }, [filters.theme, searchQuery]);

  const loadNews = async () => {
    setLoading(true);
    setError('');
    try {
      let response;
      try {
        response = await apiService.getTopNews(500);
      } catch {
        response = await apiService.getRecentNews(500);
      }
      const newsArticles = response.news || response.results || [];
      setArticles(newsArticles);
      setAllArticles(newsArticles);
    } catch (err) {
      console.error('Failed to load news:', err);
      setError(err instanceof Error ? err.message : 'Failed to load news');
    } finally {
      setLoading(false);
    }
  };

  // Client-side filtering for source and date
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
    
    // If search query is cleared in filters, also clear the main search
    if (newFilters.searchQuery === '' && filters.searchQuery !== '') {
      setSearchQuery('');
      setFeaturedArticle(null);
      loadNews();
    }
  };

  // Handle manual search (for search button click)
  const handleSearch = useCallback(() => {
    // The useEffect with debouncedSearchQuery will handle this automatically
    // This function can be used for additional search button functionality if needed
  }, []);

  return (
    <div className="min-h-screen bg-gray-50">
      <Header 
        searchQuery={searchQuery} 
        onSearchChange={setSearchQuery} 
        onSearch={handleSearch} 
      />

      <main className="max-w-65rem mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex flex-col lg:flex-row gap-8">
          <div className="flex-1">
            <NewsGrid
              featuredArticle={searchQuery ? null : featuredArticle} // Hide featured during search
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
              sources={sources}
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