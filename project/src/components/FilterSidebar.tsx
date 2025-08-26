import React, { useEffect, useState } from 'react';
import { Filter, Eye, Activity, Database } from 'lucide-react';
import { NewsFilters, Theme, Stats, Sourcename } from '../types/news';
import { apiService } from '../services/api';

interface FilterSidebarProps {
  filters: NewsFilters;
  onFiltersChange: (filters: NewsFilters) => void;
  articleCount: number;
  sources: Sourcename[];
}

const FilterSidebar: React.FC<FilterSidebarProps> = ({ filters, onFiltersChange, articleCount }) => {
  const [themes, setThemes] = useState<Theme[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [sources, setSources] = useState<Sourcename[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const [themesResponse, statsResponse, sourcesResponse] = await Promise.all([
          apiService.getThemes(),
          apiService.getStats(),
          apiService.getSourcename()
        ]);

        setThemes(themesResponse.themes || []);
        setStats(statsResponse.stats || null);
        setSources(sourcesResponse.Sourcename || []);
      } catch (error) {
        console.error('Failed to load sidebar data:', error);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, []);

  const handleFilterChange = (key: keyof NewsFilters, value: string) => {
    onFiltersChange({
      ...filters,
      [key]: value
    });
  };


  return (
    <div className="bg-white rounded-lg shadow-md p-6 sticky top-6 space-y-6">
      <div className="flex items-center mb-6">
        <Filter className="h-5 w-5 text-gray-600 mr-2" />
        <h3 className="text-lg font-semibold text-gray-900">FILTERS</h3>
      </div>

      <div className="space-y-6">
        {/* Theme Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Theme
          </label>
          <select
            value={filters.theme}
            onChange={(e) => handleFilterChange('theme', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
            disabled={loading}
          >
            <option value="All">All Themes</option>
            {themes.slice(0, 20).map((theme) => (
              <option key={theme.theme} value={theme.theme}>
                {theme.theme.replace(/_/g, ' ')} ({theme.count})
              </option>
            ))}
          </select>
        </div>

        {/* Source Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Source
          </label>
          <select
            value={filters.source}
            onChange={(e) => handleFilterChange('source', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
            disabled={loading}
          >
            <option value="All">All Sources</option>
            {sources.map((source) => (
              <option key={source.source} value={source.source}>
                {source.source} ({source.count})
              </option>
            ))}
          </select>
        </div>

        
      </div>

      {/* Current Results */}
      <div className="pt-6 border-t border-gray-200">
        <div className="flex items-center mb-4">
          <Eye className="h-5 w-5 text-gray-600 mr-2" />
          <h4 className="text-sm font-medium text-gray-700">CURRENT RESULTS</h4>
        </div>
        <div className="bg-blue-50 rounded-lg p-4 text-center">
          <div className="text-2xl font-bold text-blue-600 mb-1">{articleCount}</div>
          <div className="text-sm text-gray-600">Articles Found</div>
        </div>
      </div>

      {/* Database Stats */}
      {stats && (
        <div className="pt-6 border-t border-gray-200">
          <div className="flex items-center mb-4">
            <Database className="h-5 w-5 text-gray-600 mr-2" />
            <h4 className="text-sm font-medium text-gray-700">DATABASE STATS</h4>
          </div>
          <div className="space-y-3">
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-600">Total Events</span>
              <span className="font-medium text-gray-900">{stats.total_events.toLocaleString()}</span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-600">Total Mentions</span>
              <span className="font-medium text-gray-900">{stats.total_mentions.toLocaleString()}</span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-600">Themes</span>
              <span className="font-medium text-gray-900">{stats.total_themes.toLocaleString()}</span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-600 flex items-center">
                <Activity className="h-3 w-3 mr-1" />
                Recent (7 days)
              </span>
              <span className="font-medium text-green-600">{stats.recent_events.toLocaleString()}</span>
            </div>
          </div>
        </div>
      )}

      {/* Reset Filters */}
      <button
        onClick={() =>
          onFiltersChange({
            source: 'All',
            theme: 'All',
            dateRange: 'All Time',
            searchQuery: ''
          })
        }
        className="w-full px-4 py-2 bg-gray-100 text-gray-700 rounded-md hover:bg-gray-200 transition-colors text-sm font-medium"
      >
        Reset Filters
      </button>
    </div>
  );
};

export default FilterSidebar;
