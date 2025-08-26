import React, { useState, useEffect } from 'react';
import { Search, User, Menu, Activity, AlertCircle, CheckCircle } from 'lucide-react';
import { apiService } from '../services/api';

interface HeaderProps {
  searchQuery: string;
  onSearchChange: (query: string) => void;
  onSearch: () => void;
}

const Header: React.FC<HeaderProps> = ({ searchQuery, onSearchChange, onSearch }) => {
  const [apiStatus, setApiStatus] = useState<'checking' | 'connected' | 'disconnected'>('checking');

  useEffect(() => {
    const checkApiHealth = async () => {
      try {
        await apiService.healthCheck();
        setApiStatus('connected');
      } catch (error) {
        setApiStatus('disconnected');
      }
    };

    checkApiHealth();
    // Check every 30 seconds
    const interval = setInterval(checkApiHealth, 30000);
    return () => clearInterval(interval);
  }, []);

  const handleSearchSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSearch();
  };

  const getStatusIcon = () => {
    switch (apiStatus) {
      case 'checking':
        return <Activity className="h-4 w-4 text-yellow-500 animate-pulse" />;
      case 'connected':
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case 'disconnected':
        return <AlertCircle className="h-4 w-4 text-red-500" />;
    }
  };

  const getStatusText = () => {
    switch (apiStatus) {
      case 'checking':
        return 'Checking API...';
      case 'connected':
        return 'Connected to Flask API';
      case 'disconnected':
        return 'API Disconnected';
    }
  };

  return (
    <header className="bg-white shadow-sm border-b border-gray-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo */}
          <div className="flex-shrink-0">
            <h1 className="text-2xl font-bold text-gray-900">NewsPortal</h1>
          </div>
          
          {/* Navigation */}
          <nav className="hidden md:flex space-x-8">
            <a href="#" className="text-gray-900 font-medium hover:text-blue-600 transition-colors">
              HOME
            </a>
            <a href="#" className="text-gray-600 hover:text-gray-900 transition-colors">
              SERVICES
            </a>
            <a href="#" className="text-gray-600 hover:text-gray-900 transition-colors">
              PRODUCT
            </a>
            <a href="#" className="text-gray-600 hover:text-gray-900 transition-colors">
              ABOUT US
            </a>
          </nav>

          {/* API Status & User Profile */}
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2 text-xs">
              {getStatusIcon()}
              <span className={`hidden sm:inline ${
                apiStatus === 'connected' ? 'text-green-600' : 
                apiStatus === 'disconnected' ? 'text-red-600' : 'text-yellow-600'
              }`}>
                {getStatusText()}
              </span>
            </div>
            <button className="p-2 text-gray-600 hover:text-gray-900 transition-colors">
              <User className="h-5 w-5" />
            </button>
            <button className="md:hidden p-2 text-gray-600 hover:text-gray-900 transition-colors">
              <Menu className="h-5 w-5" />
            </button>
          </div>
        </div>
      </div>

      {/* Search Bar */}
      <div className="bg-gray-50 border-t border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <form onSubmit={handleSearchSubmit} className="relative max-w-lg mx-auto">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Search className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              placeholder="Search news articles ..."
              value={searchQuery}
              onChange={(e) => onSearchChange(e.target.value)}
              className="block w-full pl-10 pr-12 py-3 border border-gray-300 rounded-full leading-5 bg-white placeholder-gray-500 focus:outline-none focus:placeholder-gray-400 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all"
            />
            <button
              type="submit"
              className="absolute inset-y-0 right-0 pr-3 flex items-center"
            >
              <div className="bg-blue-600 hover:bg-blue-700 text-white rounded-full p-2 transition-colors">
                <Search className="h-4 w-4" />
              </div>
            </button>
          </form>
        </div>
      </div>
    </header>
  );
};

export default Header;