import React from 'react';
import ArticleCard from './ArticleCard';
import { Article } from '../types/news';
import { Loader2, AlertCircle, ExternalLink } from 'lucide-react';

interface NewsGridProps {
  articles: Article[];
  featuredArticle?: Article | null;
  loading?: boolean;
  error?: string;
}

const NewsGrid: React.FC<NewsGridProps> = ({ articles, featuredArticle, loading = false, error }) => {
  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin text-blue-600 mx-auto mb-4" />
           <div className="text-gray-800 font-medium flex items-center">
            Loading news articles
            <span className="ml-1 flex">
              <span className="animate-blink-1 ">.</span>
              <span className="animate-blink-2">.</span>
              <span className="animate-blink-3">.</span>
            </span>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="text-center">
          <AlertCircle className="h-8 w-8 text-red-500 mx-auto mb-4" />
          <div className="text-gray-600 mb-2">Failed to load news articles</div>
          <div className="text-sm text-gray-500">{error}</div>
        </div>
      </div>
    );
  }

  if (articles.length === 0 && !featuredArticle) {
    return (
      <div className="text-center py-12">
        <div className="text-gray-400 text-lg mb-2">No articles found</div>
        <div className="text-gray-500 text-sm">Try adjusting your search or filter criteria</div>
      </div>
    );
  }

  // Remove the featured article from the regular articles list if present
  const regularArticles = featuredArticle
    ? articles.filter(article => article.id !== featuredArticle.id)
    : articles;

  return (
    <div className="space-y-8">
      {/* API Connection Notice */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex items-center">
          <ExternalLink className="h-5 w-5 text-blue-600 mr-2" />
          <div>
            <div className="text-sm font-medium text-blue-800">
              Connected to Live Database
            </div>
            <div className="text-xs text-blue-600">
              Showing real news data from GDELT database. Click articles to visit original sources.
            </div>
          </div>
        </div>
      </div>

      {/* Featured Article */}
      {featuredArticle && (
        <div>
          <h2 className="text-xl font-semibold text-gray-900 mb-4 flex items-center">
            <span>Featured Story</span>
            <span className="ml-2 px-2 py-1 bg-red-100 text-red-800 text-xs font-medium rounded-full">
              {featuredArticle.num_mentions}  Mentions
            </span>
          </h2>
          <ArticleCard article={featuredArticle} featured={true} />
        </div>
      )}

      {/* Regular Articles Grid */}
      {regularArticles.length > 0 && (
        <div>
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Latest News</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {regularArticles.map((article) => (
              <ArticleCard key={article.id} article={article} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default NewsGrid;