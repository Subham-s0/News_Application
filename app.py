# app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta

import os
from functools import lru_cache

# Initialize Flask app
app = Flask(__name__)
CORS(app, origins=["http://localhost:5173"])

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'News'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '1234')
}

class DatabaseManager:
    def __init__(self):
        self.db_config = DB_CONFIG
    
    def get_connection(self):
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            if "could not connect to server" in str(e).lower():
                logger.error("PostgreSQL service might not be running")
            elif "authentication failed" in str(e).lower():
                logger.error("Check database username and password")
            elif "database" in str(e).lower() and "does not exist" in str(e).lower():
                logger.error("Database 'News' does not exist")
            return None
        except Exception as e:
            logger.error(f"Unexpected database error: {e}")
            return None
    
    def execute_query(self, query, params=None, fetch=True):
        conn = self.get_connection()
        if not conn:
            logger.error("Could not establish database connection")
            return None
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logger.debug(f"Executing query: {query[:100]}...")
                if params:
                    logger.debug(f"Query parameters: {params}")
                
                cursor.execute(query, params)
                
                if fetch:
                    results = cursor.fetchall()
                    logger.debug(f"Query returned {len(results)} rows")
                    return results
                else:
                    conn.commit()
                    logger.debug("Query executed successfully (no fetch)")
                    return True
                    
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL error: {e}")
            logger.error(f"Error code: {e.pgcode}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            conn.rollback()
            return None
        except Exception as e:
            logger.error(f"Unexpected query error: {e}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            conn.rollback()
            return None
        finally:
            conn.close()

class NewsSearchEngine:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.vectorizer = None
        self.nn_model = None
        self.news_data = None
        self.last_updated = None
        self.update_interval = timedelta(hours=1)  # Update every hour
    
    def extract_page_title(self, extras):
        """Extract page title from extras field"""
        if not extras:
            return ""
        
        match = re.search(r'<pageTitle>(.*?)</pageTitle>', extras, re.IGNORECASE)
        return match.group(1) if match else ""
    
    def preprocess_text(self, text_list):
        """Clean and preprocess text data"""
        processed = []
        for text in text_list:
            if text:
                # Clean and normalize text
                text = re.sub(r'[^\w\s]', ' ', str(text))
                text = ' '.join(text.split())
                processed.append(text.lower())
            else:
                processed.append("")
        return processed
    
    def load_news_data(self):
        """Load and prepare news data for TF-IDF"""
        query = """
        SELECT DISTINCT
            e.GlobalEventID,
            e.SQLDATE as date,
            e.Actor1Name,
            e.Actor2Name,
            e.NumMentions,
            g.SourceCommonName,
            g.SharingImage as image,
            g.Extras,
            COALESCE(gt.theme, '') as theme,
            COALESCE(gp.person, '') as person,
            COALESCE(go.organization, '') as organization,
            COALESCE(gn.name, '') as name,
            m.Confidence
        FROM events e
        LEFT JOIN gkg g ON e.GlobalEventID = g.GKGRECORDID
        LEFT JOIN gkg_themes gt ON g.GKGRECORDID = gt.GKGRECORDID
        LEFT JOIN gkg_persons gp ON g.GKGRECORDID = gp.GKGRECORDID  
        LEFT JOIN gkg_organizations go ON g.GKGRECORDID = go.GKGRECORDID
        LEFT JOIN gkg_names gn ON g.GKGRECORDID = gn.GKGRECORDID
        LEFT JOIN mentions m ON e.GlobalEventID = m.GlobalEventID
        ORDER BY e.SQLDATE DESC
        LIMIT 10000
        """
        
        results = self.db_manager.execute_query(query)
        if not results:
            return False
        
        # Convert to DataFrame and process
        df = pd.DataFrame([dict(row) for row in results])
        
        # Extract page titles
        df['title'] = df['extras'].apply(self.extract_page_title)
        
        # Combine text fields for TF-IDF
        df['combined_text'] = (
            df['title'].fillna('') + ' ' +
            df['theme'].fillna('') + ' ' +
            df['person'].fillna('') + ' ' +
            df['organization'].fillna('') + ' ' +
            df['name'].fillna('') + ' ' +
            df['actor1name'].fillna('') + ' ' +
            df['actor2name'].fillna('')
        )
        
        self.news_data = df
        return True
    
    def build_search_index(self):
        """Build TF-IDF index and KNN model"""
        if self.news_data is None or len(self.news_data) == 0:
            return False
        
        try:
            # Preprocess combined text
            texts = self.preprocess_text(self.news_data['combined_text'].tolist())
            
            # Create TF-IDF vectorizer
            self.vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words='english',
                ngram_range=(1, 2),
                min_df=2,
                max_df=0.8
            )
            
            # Fit and transform texts
            tfidf_matrix = self.vectorizer.fit_transform(texts)
            
            # Build KNN model
            self.nn_model = NearestNeighbors(
                n_neighbors=min(20, len(texts)),
                algorithm='auto',
                metric='cosine'
            )
            self.nn_model.fit(tfidf_matrix)
            
            self.last_updated = datetime.now()
            logger.info("Search index built successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to build search index: {e}")
            return False
    
    def should_update_index(self):
        """Check if index needs updating"""
        if self.last_updated is None:
            return True
        return datetime.now() - self.last_updated > self.update_interval
    
    def search(self, query, limit=10):
        """Search news using TF-IDF and KNN"""
        if self.should_update_index():
            self.load_news_data()
            self.build_search_index()
        
        if not self.vectorizer or not self.nn_model:
            return []
        
        try:
            # Transform query
            query_processed = self.preprocess_text([query])[0]
            query_vector = self.vectorizer.transform([query_processed])
            
            # Find nearest neighbors
            distances, indices = self.nn_model.kneighbors(query_vector, n_neighbors=limit)
            
            # Get results
            results = []
            for i, idx in enumerate(indices[0]):
                row = self.news_data.iloc[idx]
                result = {
                    'id': row['globaleventid'],
                    'title': row['title'] or 'No title available',
                    'date': row['date'],
                    'theme': row['theme'],
                    'actor1_name': row['actor1name'],
                    'actor2_name': row['actor2name'],
                    'num_mentions': row['nummentions'],
                    'confidence': row['confidence'],
                    'source': row['sourcecommonname'],
                    'image': row['image'],
                    'similarity_score': float(1 - distances[0][i])
                }
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

# Initialize components
db_manager = DatabaseManager()
search_engine = NewsSearchEngine(db_manager)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


@app.route('/api/featured', methods=['GET'])
def get_featured_news():
    """Get the top featured news item"""
    limit = request.args.get('limit', 1, type=int)  # usually 1 for top news

    query = """
    SELECT
    e.globaleventid AS id,
    e.actor1name AS actor1_name,
    e.actor2name AS actor2_name,
    e.dateadded,
    TO_DATE(e.sqldate::text, 'YYYYMMDD') AS event_date,
    e.eventcode,
    e.nummentions,
    e.avgtone,
    e.sourceurl AS url,
    g.documentidentifier,
    m.confidence,
    g.sourcecommonname,
    SPLIT_PART(g.themes, ';', 1) AS first_theme,
    g.sharingimage AS image,
    COALESCE(SUBSTRING(g.extras FROM '<PAGE_TITLE>(.*?)</PAGE_TITLE>'), 'News Article') AS title
    FROM events e
    INNER JOIN gkg g ON e.sourceurl = g.documentidentifier
    INNER JOIN mentions m ON e.globaleventid = m.globaleventid
    WHERE e.nummentions > 0
      AND e.avgtone IS NOT NULL
      AND g.sharingimage IS NOT NULL
    ORDER BY (e.nummentions / 3.0) * e.avgtone DESC
    LIMIT %s;

    """

    results = db_manager.execute_query(query, (limit,))
    if results is None:
        return jsonify({'error': 'Database query failed'}), 500

    news_data = [dict(row) for row in results]
    return jsonify({'news': news_data, 'total': len(news_data)})


@app.route('/api/news/top', methods=['GET'])


def get_top_news():
    """Get top news based on mentions, average tone, and confidence."""
    limit = request.args.get('limit', 5000, type=int)
    
    query = """
    WITH ranked AS (
        SELECT
            e.globaleventid,
            e.actor1name,
            e.actor2name,
            e.dateadded,
            TO_DATE(e.sqldate::text, 'YYYYMMDD') AS event_date,
            e.eventcode,
            e.nummentions,
            e.avgtone,
            e.sourceurl AS url,
            g.documentidentifier,
            m.confidence,
            g.sourcecommonname,
            SPLIT_PART(g.themes, ';', 1) AS first_theme,
            g.sharingimage,
            SUBSTRING(g.extras FROM '<PAGE_TITLE>(.*?)</PAGE_TITLE>') AS page_title,
            ROW_NUMBER() OVER (
                PARTITION BY e.avgtone, e.eventcode, COALESCE(e.actor1name, e.actor2name)
                ORDER BY e.nummentions DESC, ABS(m.confidence) DESC, e.avgtone DESC
            ) AS rn
        FROM events e
        INNER JOIN gkg g ON e.sourceurl = g.documentidentifier
        INNER JOIN mentions m ON e.globaleventid = m.globaleventid
        WHERE e.nummentions > 0
          AND e.avgtone IS NOT NULL
          AND m.confidence IS NOT NULL
          AND g.sharingimage IS NOT NULL
    )
    , unique_urls AS (
        SELECT DISTINCT ON (url) *
        FROM ranked
        WHERE rn = 1
        ORDER BY url, nummentions DESC, ABS(confidence) DESC, avgtone DESC
    )
    SELECT *
    FROM unique_urls
    ORDER BY nummentions DESC, avgtone DESC, ABS(confidence) DESC
    LIMIT %s;
    """
    
    results = db_manager.execute_query(query, (limit,))
    
    if results is None:
        return jsonify({'error': 'Database query failed'}), 500
    
    news_data = []
    for row in results:
        news_data.append({
            'id': row['globaleventid'],
            'actor1_name': row['actor1name'],
            'actor2_name': row['actor2name'],
            'event_date': row['event_date'],
            'num_mentions': row['nummentions'],
            'avg_tone': row['avgtone'],
            'confidence': row['confidence'],
            'source_url': row['url'],
            'source': row['sourcecommonname'],
            'theme': row['first_theme'],
            'image': row['sharingimage'],
            'title': row['page_title']
        })
    
    return jsonify({'news': news_data, 'total': len(news_data)})


@app.route('/api/themes', methods=['GET'])
def get_themes():
    """Get available themes for filtering"""
    query = """
    SELECT theme, COUNT(*) as count
    FROM gkg_themes
    WHERE theme IS NOT NULL AND theme != ''
    GROUP BY theme
    ORDER BY count DESC
    LIMIT 100
    """
    
    results = db_manager.execute_query(query)
    if results is None:
        return jsonify({'error': 'Database query failed'}), 500
    
    themes = [dict(row) for row in results]
    return jsonify({'themes': themes})

@app.route('/api/sourcename', methods=['GET'])
def get_sources():
    """Get top 20 news sources for filtering"""
    query = """
    SELECT sourcecommonname AS source, COUNT(*) as count
    FROM gkg
    WHERE sourcecommonname IS NOT NULL AND sourcecommonname != ''
    GROUP BY sourcecommonname
    ORDER BY count DESC
    LIMIT 20
    """
    
    results = db_manager.execute_query(query)
    if results is None:
        return jsonify({'error': 'Database query failed'}), 500
    
    sourcename = [dict(row) for row in results]
    return jsonify({'Sourcename': sourcename})


@app.route('/api/news/filter', methods=['GET'])
def filter_news():
    """Filter news by theme"""
    theme = request.args.get('theme')
    limit = request.args.get('limit', 50, type=int)
    
    if not theme:
        return jsonify({'error': 'Theme parameter is required'}), 400
    
    query = """
   SELECT *
        FROM (
            SELECT DISTINCT ON (page_title)
                e.globaleventid,
                e.actor1name,
                e.actor2name,
                e.dateadded,
                TO_DATE(e.sqldate::text, 'YYYYMMDD') AS event_date,
                e.eventcode,
                e.nummentions,
                e.avgtone,
                e.sourceurl AS url,
                g.documentidentifier,
                m.confidence,
                g.sourcecommonname,
                SPLIT_PART(g.themes, ';', 1) AS first_theme,
                g.sharingimage,
                SUBSTRING(g.extras FROM '<PAGE_TITLE>(.*?)</PAGE_TITLE>') AS page_title
            FROM gkg g
            INNER JOIN events e ON g.documentidentifier = e.sourceurl
            INNER JOIN mentions m ON e.globaleventid = m.globaleventid
            INNER JOIN gkg_themes gt ON g.gkgrecordid = gt.gkgrecordid
            WHERE gt.theme ILIKE %s
            ORDER BY page_title, e.nummentions DESC
        ) t
        ORDER BY t.nummentions DESC
        LIMIT %s;
    """
    
    results = db_manager.execute_query(query, (f'%{theme}%', limit))
    if results is None:
        return jsonify({'error': 'Database query failed'}), 500
    news_data = []
    for row in results:
        news_data.append({
            'id': row['globaleventid'],
            'actor1_name': row['actor1name'],
            'actor2_name': row['actor2name'],
            'event_date': row['event_date'],
            'num_mentions': row['nummentions'],
            'avg_tone': row['avgtone'],
            'confidence': row['confidence'],
            'source_url': row['url'],
            'source': row['sourcecommonname'],
            'theme': row['first_theme'],
            'image': row['sharingimage'],
            'title': row['page_title']
        })
    
    return jsonify({'news': news_data, 'total': len(news_data)})

@app.route('/api/search', methods=['GET'])
def search_news():
    """Search news using TF-IDF"""
    query = request.args.get('q', '').strip()
    limit = request.args.get('limit', 10, type=int)
    
    if not query:
        return jsonify({'error': 'Search query is required'}), 400
    
    try:
        results = search_engine.search(query, limit)
        return jsonify({'results': results, 'total': len(results), 'query': query})
    except Exception as e:
        logger.error(f"Search API failed: {e}")
        return jsonify({'error': 'Search failed'}), 500
@app.route('/api/news/<news_id>', methods=['GET'])
def get_news_detail(news_id):
    """Get detailed information about a specific news item"""
    query = """
    SELECT DISTINCT
        e.GlobalEventID as id,
        e.SQLDATE as date,
        e.Actor1Name as actor1_name,
        e.Actor2Name as actor2_name,
        e.NumMentions as num_mentions,
        e.GoldsteinScale as goldstein_scale,
        e.AvgTone as avg_tone,
        g.SourceCommonName as source,
        g.SharingImage as image,
        g.DocumentIdentifier as document_url,
        COALESCE(
            (regexp_matches(g.Extras, '<pageTitle>(.*?)</pageTitle>'))[1], 
            'News Article'
        ) AS title,
        STRING_AGG(DISTINCT gt.theme, ', ') as themes,
        STRING_AGG(DISTINCT gp.person, ', ') as persons,
        STRING_AGG(DISTINCT go.organization, ', ') as organizations,
        AVG(m.Confidence) as avg_confidence
    FROM events e
    LEFT JOIN gkg g ON e.GlobalEventID = g.GKGRECORDID
    LEFT JOIN gkg_themes gt ON g.GKGRECORDID = gt.GKGRECORDID
    LEFT JOIN gkg_persons gp ON g.GKGRECORDID = gp.GKGRECORDID  
    LEFT JOIN gkg_organizations go ON g.GKGRECORDID = go.GKGRECORDID
    LEFT JOIN mentions m ON e.GlobalEventID = m.GlobalEventID
    WHERE e.GlobalEventID = %s
    GROUP BY e.GlobalEventID, e.SQLDATE, e.Actor1Name, e.Actor2Name, 
             e.NumMentions, e.GoldsteinScale, e.AvgTone, g.SourceCommonName, 
             g.SharingImage, g.DocumentIdentifier, g.Extras
    """
    
    results = db_manager.execute_query(query, (news_id,))

    if not results:

    
        return jsonify({'error': 'News item not found'}), 404
    
    news_data = []
    for row in results:
        news_data.append({
            'id': row['globaleventid'],
            'actor1_name': row['actor1name'],
            'actor2_name': row['actor2name'],
            'event_date': row['event_date'],
            'num_mentions': row['nummentions'],
            'avg_tone': row['avgtone'],
            'confidence': row['confidence'],
            'source_url': row['url'],
            'source': row['sourcecommonname'],
            'theme': row['first_theme'],
            'image': row['sharingimage'],
            'title': row['page_title']
        })
    
    return jsonify({'news': news_data})

@app.route('/api/debug/tables', methods=['GET'])
def debug_tables():
    """Debug endpoint to check table contents and relationships"""
    debug_info = {}
    
    try:
        # Check events table
        events_query = "SELECT COUNT(*) as count FROM events"
        events_result = db_manager.execute_query(events_query)
        debug_info['events_count'] = events_result[0]['count'] if events_result else 0
        
        # Get sample events
        sample_events_query = "SELECT GlobalEventID, SQLDATE, Actor1Name, Actor2Name, NumMentions FROM events LIMIT 5"
        sample_events = db_manager.execute_query(sample_events_query)
        debug_info['sample_events'] = [dict(row) for row in sample_events] if sample_events else []
        
        # Check mentions table
        mentions_query = "SELECT COUNT(*) as count FROM mentions"
        mentions_result = db_manager.execute_query(mentions_query)
        debug_info['mentions_count'] = mentions_result[0]['count'] if mentions_result else 0
        
        # Get sample mentions
        sample_mentions_query = "SELECT GlobalEventID, MentionsSource, Confidence FROM mentions LIMIT 5"
        sample_mentions = db_manager.execute_query(sample_mentions_query)
        debug_info['sample_mentions'] = [dict(row) for row in sample_mentions] if sample_mentions else []
        
        # Check gkg table
        gkg_query = "SELECT COUNT(*) as count FROM gkg"
        gkg_result = db_manager.execute_query(gkg_query)
        debug_info['gkg_count'] = gkg_result[0]['count'] if gkg_result else 0
        
        # Get sample gkg
        sample_gkg_query = "SELECT GKGRECORDID, SourceCommonName, SharingImage FROM gkg LIMIT 5"
        sample_gkg = db_manager.execute_query(sample_gkg_query)
        debug_info['sample_gkg'] = [dict(row) for row in sample_gkg] if sample_gkg else []
        
        # Check gkg_themes table
        themes_query = "SELECT COUNT(*) as count FROM gkg_themes"
        themes_result = db_manager.execute_query(themes_query)
        debug_info['themes_count'] = themes_result[0]['count'] if themes_result else 0
        
        # Check for common IDs between events and gkg
        common_ids_query = """
        SELECT COUNT(*) as count 
        FROM events e 
        INNER JOIN gkg g ON e.GlobalEventID = g.GKGRECORDID
        """
        common_ids_result = db_manager.execute_query(common_ids_query)
        debug_info['common_event_gkg_ids'] = common_ids_result[0]['count'] if common_ids_result else 0
        
        # Check for common IDs between events and mentions
        common_mentions_query = """
        SELECT COUNT(*) as count 
        FROM events e 
        INNER JOIN mentions m ON e.GlobalEventID = m.GlobalEventID
        """
        common_mentions_result = db_manager.execute_query(common_mentions_query)
        debug_info['common_event_mention_ids'] = common_mentions_result[0]['count'] if common_mentions_result else 0
        
        return jsonify({'debug_info': debug_info})
        
    except Exception as e:
        logger.error(f"Debug endpoint failed: {e}")
        return jsonify({'error': f'Debug failed: {str(e)}'}), 500

@app.route('/api/test-simple', methods=['GET'])
def test_simple_query():
    """Test with the simplest possible query"""
    try:
        query = "SELECT GlobalEventID, SQLDATE, Actor1Name, NumMentions FROM events LIMIT 10"
        results = db_manager.execute_query(query)
        
        if results is None:
            return jsonify({'error': 'Query returned None'}), 500
        
        data = [dict(row) for row in results]
        return jsonify({'success': True, 'data': data, 'count': len(data)})
        
    except Exception as e:
        logger.error(f"Simple test query failed: {e}")
        return jsonify({'error': f'Simple query failed: {str(e)}'}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    queries = {
        'total_events': "SELECT COUNT(*) as count FROM events",
        'total_mentions': "SELECT COUNT(*) as count FROM mentions",
        'total_gkg': "SELECT COUNT(*) as count FROM gkg",
        'total_themes': "SELECT COUNT(DISTINCT theme) as count FROM gkg_themes WHERE theme IS NOT NULL",
        'recent_events': "SELECT COUNT(*) as count FROM events WHERE SQLDATE >= %s"
    }
    
    # Calculate date for recent events (last 7 days)
    current_date = datetime.now()
    week_ago = int((current_date - timedelta(days=7)).strftime('%Y%m%d'))
    
    stats = {}
    for stat_name, query in queries.items():
        try:
            if stat_name == 'recent_events':
                result = db_manager.execute_query(query, (week_ago,))
            else:
                result = db_manager.execute_query(query)
            
            if result and len(result) > 0:
                stats[stat_name] = result[0]['count']
            else:
                stats[stat_name] = 0
        except Exception as e:
            logger.warning(f"Failed to get stat {stat_name}: {e}")
            stats[stat_name] = 0
    
    return jsonify({'stats': stats})

if __name__ == '__main__':
    # Test database connection on startup
    logger.info("Testing database connection...")
    db_manager_test = DatabaseManager()
    
    # Test with simple query
    test_result = db_manager_test.execute_query("SELECT 1 as test")
    if test_result:
        logger.info("✅ Database connection successful!")
        
        # Initialize search engine
        logger.info("Initializing search engine...")
        try:
            if search_engine.load_news_data():
                search_engine.build_search_index()
                logger.info("✅ Search engine initialized successfully")
            else:
                logger.warning("⚠️ Failed to initialize search engine - will work without search functionality")
        except Exception as e:
            logger.warning(f"⚠️ Search engine initialization failed: {e}")
    else:
        logger.error("❌ Database connection failed!")
        logger.error("Please check your database configuration and ensure PostgreSQL is running")
        logger.error("You can still start the Flask app, but database endpoints will fail")
    
    logger.info(f"🚀 Starting Flask application on port 8000...")
    logger.info(f"🔗 Access the API at: http://localhost:8000/api/")
    logger.info(f"🔍 Test endpoints:")
    logger.info(f"   - Health check: http://localhost:8000/api/health")
    logger.info(f"   - Simple test: http://localhost:8000/api/test-simple") 
    logger.info(f"   - Debug info: http://localhost:8000/api/debug/tables")
    logger.info(f"   - Recent news: http://localhost:8000/api/news/recent")
    
    app.run(host='0.0.0.0', port=8000, debug=True)