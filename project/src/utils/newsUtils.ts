import { Article } from '../types/news';

export const formatDate = (dateString: string | undefined): string => {
  if (!dateString) return 'Unknown date';
  
  // Handle YYYYMMDD format from database
  if (dateString.length === 8 && /^\d{8}$/.test(dateString)) {
    const year = dateString.substring(0, 4);
    const month = dateString.substring(4, 6);
    const day = dateString.substring(6, 8);
    const date = new Date(`${year}-${month}-${day}`);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    });
  }
  
  // Handle ISO date format
  try {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    });
  } catch {
    return dateString;
  }
};

export const getThemeColor = (theme: string | undefined): string => {
  if (!theme) return 'bg-gray-100 text-gray-800';
  
  const colors: Record<string, string> = {
    'GENERAL_GOVERNMENT': 'bg-blue-100 text-blue-800',
    'MILITARY': 'bg-red-100 text-red-800',
    'SCIENCE_TECHNOLOGY': 'bg-purple-100 text-purple-800',
    'EDUCATION': 'bg-green-100 text-green-800',
    'HEALTH': 'bg-pink-100 text-pink-800',
    'ECONOMY': 'bg-yellow-100 text-yellow-800',
    'ENVIRONMENT': 'bg-emerald-100 text-emerald-800',
    'SPORTS': 'bg-orange-100 text-orange-800',
    'CRIME': 'bg-red-200 text-red-900',
    'POLITICS': 'bg-indigo-100 text-indigo-800',
  };
  
  // Check for exact match first
  if (colors[theme.toUpperCase()]) {
    return colors[theme.toUpperCase()];
  }
  
  // Check for partial matches
  const themeUpper = theme.toUpperCase();
  for (const [key, color] of Object.entries(colors)) {
    if (themeUpper.includes(key) || key.includes(themeUpper)) {
      return color;
    }
  }
  
  return 'bg-gray-100 text-gray-800';
};

export const generateTitle = (article: Article): string => {
  if (article.title && article.title !== 'News Event' && article.title !== 'Breaking News Event') {
    return article.title;
  }
  
  // Generate title from actors and theme
  const parts = [];
  if (article.actor1_name) parts.push(article.actor1_name);
  if (article.actor2_name && article.actor2_name !== article.actor1_name) {
    parts.push(article.actor2_name);
  }
  if (article.theme) parts.push(`(${article.theme})`);
  
  return parts.length > 0 ? parts.join(' - ') : 'News Event';
};

export const generateExcerpt = (article: Article): string => {
  const parts = [];
  
  if (article.actor1_name && article.actor2_name) {
    parts.push(`Event involving ${article.actor1_name} and ${article.actor2_name}`);
  } else if (article.actor1_name) {
    parts.push(`Event involving ${article.actor1_name}`);
  }
  
  if (article.theme) {
    parts.push(`Related to ${article.theme.toLowerCase()}`);
  }
  
  if (article.num_mentions > 1) {
    parts.push(`Mentioned ${article.num_mentions} times`);
  }
  
  if (article.avg_tone !== undefined) {
    const tone = article.avg_tone > 0 ? 'positive' : article.avg_tone < 0 ? 'negative' : 'neutral';
    parts.push(`Overall tone: ${tone}`);
  }
  
  return parts.length > 0 ? parts.join('. ') + '.' : 'Click to read more details about this news event.';
};

export const openNewsUrl = (article: Article): void => {
  const url = article.source_url || article.document_url;
  if (url) {
    // Ensure URL has protocol
    const fullUrl = url.startsWith('http') ? url : `https://${url}`;
    window.open(fullUrl, '_blank', 'noopener,noreferrer');
  }
};

export const getImageUrl = (article: Article): string => {
  if (article.image && article.image.startsWith('http')) {
    return article.image;
  }
  
  // Fallback images based on theme
  const themeImages: Record<string, string> = {
    'GENERAL_GOVERNMENT': 'https://images.pexels.com/photos/8386440/pexels-photo-8386440.jpeg?auto=compress&cs=tinysrgb&w=800',
    'MILITARY': 'https://images.pexels.com/photos/1263348/pexels-photo-1263348.jpeg?auto=compress&cs=tinysrgb&w=800',
    'SCIENCE_TECHNOLOGY': 'https://images.pexels.com/photos/586063/pexels-photo-586063.jpeg?auto=compress&cs=tinysrgb&w=800',
    'EDUCATION': 'https://images.pexels.com/photos/4265906/pexels-photo-4265906.jpeg?auto=compress&cs=tinysrgb&w=800',
    'HEALTH': 'https://images.pexels.com/photos/8386440/pexels-photo-8386440.jpeg?auto=compress&cs=tinysrgb&w=800',
    'ECONOMY': 'https://images.pexels.com/photos/6801874/pexels-photo-6801874.jpeg?auto=compress&cs=tinysrgb&w=800',
    'ENVIRONMENT': 'https://images.pexels.com/photos/9800029/pexels-photo-9800029.jpeg?auto=compress&cs=tinysrgb&w=800',
    'SPORTS': 'https://images.pexels.com/photos/1263348/pexels-photo-1263348.jpeg?auto=compress&cs=tinysrgb&w=800',
  };
  
  if (article.theme) {
    const themeUpper = article.theme.toUpperCase();
    for (const [key, image] of Object.entries(themeImages)) {
      if (themeUpper.includes(key) || key.includes(themeUpper)) {
        return image;
      }
    }
  }
  
  // Default fallback
  return 'https://images.pexels.com/photos/4265906/pexels-photo-4265906.jpeg?auto=compress&cs=tinysrgb&w=800';
};