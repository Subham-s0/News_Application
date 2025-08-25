import React from 'react';
import { Calendar, User, Tag, ExternalLink, TrendingUp, MessageCircle } from 'lucide-react';
import { Article } from '../types/news';
import { formatDate, getThemeColor, generateTitle, generateExcerpt, openNewsUrl, getImageUrl } from '../utils/newsUtils';

interface ArticleCardProps {
  article: Article;
  featured?: boolean;
}

const ArticleCard: React.FC<ArticleCardProps> = ({ article, featured = false }) => {
  const title = generateTitle(article);
  const excerpt = generateExcerpt(article);
  const imageUrl = getImageUrl(article);
  const hasUrl = !!(article.source_url || article.document_url);

  const handleCardClick = () => {
    if (hasUrl) {
      openNewsUrl(article);
    }
  };

  if (featured) {
    return (
      <div 
        className={`bg-white rounded-xl shadow-lg overflow-hidden hover:shadow-xl transition-all duration-300 transform hover:-translate-y-1 ${hasUrl ? 'cursor-pointer' : ''}`}
        onClick={handleCardClick}
      >
        <div className="relative">
          <img
            src={imageUrl}
            alt={title}
            className="w-full h-64 object-cover"
            onError={(e) => {
              const target = e.target as HTMLImageElement;
              target.src = 'https://images.pexels.com/photos/4265906/pexels-photo-4265906.jpeg?auto=compress&cs=tinysrgb&w=800';
            }}
          />
          <div className="absolute top-4 left-4">
            {article.theme && (
              <span className={`inline-flex items-center px-3 py-1 rounded-full text-xs font-medium ${getThemeColor(article.theme)}`}>
                <Tag className="h-3 w-3 mr-1" />
                {article.theme.replace(/_/g, ' ')}
              </span>
            )}
          </div>
          {hasUrl && (
            <div className="absolute top-4 right-4">
              <div className="bg-white/90 backdrop-blur-sm rounded-full p-2">
                <ExternalLink className="h-4 w-4 text-gray-600" />
              </div>
            </div>
          )}
        </div>
        
        <div className="p-6">
          <h2 className="text-2xl font-bold text-gray-900 mb-3 line-clamp-2 hover:text-blue-600 transition-colors">
            {title}
          </h2>
          
          <p className="text-gray-600 mb-4 line-clamp-3 leading-relaxed">
            {excerpt}
          </p>

          <div className="flex items-center justify-between text-sm text-gray-500 mb-4">
            <div className="flex items-center space-x-4">
              <div className="flex items-center">
                <Calendar className="h-4 w-4 mr-1" />
                {formatDate(article.event_date)}
              </div>
              {article.actor1_name && (
                <div className="flex items-center">
                  <User className="h-4 w-4 mr-1" />
                  {article.actor1_name}
                </div>
              )}
            </div>
            <div className="text-sm font-medium text-blue-600">
              {article.source || 'Unknown Source'}
            </div>
          </div>

          {/* Metrics */}
          <div className="flex items-center space-x-4 text-xs text-gray-500">
            {article.num_mentions > 0 && (
              <div className="flex items-center">
                <MessageCircle className="h-3 w-3 mr-1" />
                {article.num_mentions} mentions
              </div>
            )}
            {article.confidence !== undefined && (
              <div className="flex items-center">
                <TrendingUp className="h-3 w-3 mr-1" />
                {Math.abs(article.confidence).toFixed(1)}% confidence
              </div>
            )}
            {article.avg_tone !== undefined && (
              <div className={`px-2 py-1 rounded-full text-xs font-medium ${
                article.avg_tone > 0 ? 'bg-green-100 text-green-800' : 
                article.avg_tone < 0 ? 'bg-red-100 text-red-800' : 
                'bg-gray-100 text-gray-800'
              }`}>
                {article.avg_tone > 0 ? 'Positive' : article.avg_tone < 0 ? 'Negative' : 'Neutral'} tone
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div 
      className={`bg-white rounded-lg shadow-md overflow-hidden hover:shadow-lg transition-all duration-300 transform hover:-translate-y-0.5 ${hasUrl ? 'cursor-pointer' : ''}`}
      onClick={handleCardClick}
    >
      <div className="relative">
        <img
          src={imageUrl}
          alt={title}
          className="w-full h-48 object-cover"
          onError={(e) => {
            const target = e.target as HTMLImageElement;
            target.src = 'https://images.pexels.com/photos/4265906/pexels-photo-4265906.jpeg?auto=compress&cs=tinysrgb&w=800';
          }}
        />
        <div className="absolute top-3 left-3">
          {article.theme && (
            <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${getThemeColor(article.theme)}`}>
              {article.theme.replace(/_/g, ' ')}
            </span>
          )}
        </div>
        {hasUrl && (
          <div className="absolute top-3 right-3">
            <div className="bg-white/90 backdrop-blur-sm rounded-full p-1.5">
              <ExternalLink className="h-3 w-3 text-gray-600" />
            </div>
          </div>
        )}
      </div>
      
      <div className="p-4">
        <h3 className="text-lg font-semibold text-gray-900 mb-2 line-clamp-2 hover:text-blue-600 transition-colors">
          {title}
        </h3>
        
        <p className="text-gray-600 text-sm mb-3 line-clamp-2">
          {excerpt}
        </p>

        <div className="flex items-center justify-between text-xs text-gray-500 mb-2">
          <div className="flex items-center space-x-2">
            <span>{formatDate(article.event_date)}</span>
            {article.actor1_name && (
              <>
                <span>•</span>
                <span>{article.actor1_name}</span>
              </>
            )}
          </div>
          <span className="text-blue-600 font-medium">{article.source || 'Unknown'}</span>
        </div>

        {/* Metrics */}
        <div className="flex items-center space-x-3 text-xs text-gray-500">
          {article.num_mentions > 0 && (
            <div className="flex items-center">
              <MessageCircle className="h-3 w-3 mr-1" />
              {article.num_mentions}
            </div>
          )}
          {article.confidence !== undefined && (
            <div className="flex items-center">
              <TrendingUp className="h-3 w-3 mr-1" />
              {Math.abs(article.confidence).toFixed(1)}%
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ArticleCard;