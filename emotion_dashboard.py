import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from pymongo import MongoClient
import json
from datetime import datetime, timedelta
import numpy as np

class EmotionDashboard:
    def __init__(self):
        # MongoDB connection
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['twitter_emotions']
        self.collection = self.db['emotion_analysis']
        self.stats_collection = self.db['real_time_stats']  # Collection for real-time stats
        
        # Initialize Dash app
        self.app = dash.Dash(__name__)
        self.setup_layout()
        self.setup_callbacks()
    
    def get_latest_data(self, limit=100):
        """
        Get latest emotion data from MongoDB for visualizations
        """
        try:
            cursor = self.collection.find().sort("timestamp", -1).limit(limit)
            data = list(cursor)
            
            if data:
                df = pd.DataFrame(data)
                # Convert timestamp string to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                return df
            else:
                # Return empty DataFrame with expected columns
                return pd.DataFrame(columns=[
                    'tweet_id', 'timestamp', 'original_text', 'dominant_emotion',
                    'emotion_confidence', 'sentiment_label', 'topic', 'location',
                    'engagement_score', 'virality_potential'
                ])
        except Exception as e:
            print(f"Error fetching data: {e}")
            return pd.DataFrame()
    
    def get_real_time_stats(self):
        """
        Get real-time cumulative statistics from MongoDB
        """
        try:
            stats_doc = self.stats_collection.find_one({"stats_type": "current"})
            if stats_doc:
                return stats_doc
            else:
                # Fallback to counting all documents if no real-time stats
                total_count = self.collection.count_documents({})
                return {
                    "total_tweets_processed": total_count,
                    "total_high_confidence": 0,
                    "emotion_distribution": {},
                    "sentiment_distribution": {},
                    "avg_emotion_confidence": 0.0,
                    "most_emotional_tweet": {"emotion": "unknown", "confidence": 0.0, "text": ""}
                }
        except Exception as e:
            print(f"Error fetching real-time stats: {e}")
            return {
                "total_tweets_processed": 0,
                "total_high_confidence": 0,
                "emotion_distribution": {},
                "sentiment_distribution": {},
                "avg_emotion_confidence": 0.0,
                "most_emotional_tweet": {"emotion": "unknown", "confidence": 0.0, "text": ""}
            }
    
    def setup_layout(self):
        """
        Setup dashboard layout
        """
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1("🎭 Real-time Twitter Emotion Analysis Dashboard", 
                       style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '30px'}),
                html.P("Advanced emotion detection beyond basic sentiment analysis", 
                      style={'textAlign': 'center', 'fontSize': '18px', 'color': '#7f8c8d'})
            ], style={'backgroundColor': '#ecf0f1', 'padding': '20px', 'marginBottom': '30px'}),
            
            # Stats Row
            html.Div(id='stats-row', children=[], style={'marginBottom': '30px'}),
            
            # Main Content
            html.Div([
                # Left Column - Charts
                html.Div([
                    # Emotion Distribution Pie Chart
                    html.Div([
                        html.H3("🎯 Emotion Distribution", style={'textAlign': 'center'}),
                        dcc.Graph(id='emotion-pie-chart')
                    ], style={'backgroundColor': 'white', 'padding': '15px', 'marginBottom': '20px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'}),
                    
                    # Emotion Timeline
                    html.Div([
                        html.H3("📈 Emotion Timeline", style={'textAlign': 'center'}),
                        dcc.Graph(id='emotion-timeline')
                    ], style={'backgroundColor': 'white', 'padding': '15px', 'marginBottom': '20px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'}),
                    
                    # Sentiment vs Emotion Heatmap
                    html.Div([
                        html.H3("🌡️ Topic-Emotion Heatmap", style={'textAlign': 'center'}),
                        dcc.Graph(id='topic-emotion-heatmap')
                    ], style={'backgroundColor': 'white', 'padding': '15px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'})
                ], style={'width': '60%', 'display': 'inline-block', 'verticalAlign': 'top', 'paddingRight': '20px'}),
                
                # Right Column - Live Feed
                html.Div([
                    html.H3("🔴 Live Tweet Feed", style={'textAlign': 'center', 'color': '#e74c3c'}),
                    html.Div(id='live-tweets', style={'height': '600px', 'overflowY': 'scroll'})
                ], style={'width': '38%', 'display': 'inline-block', 'verticalAlign': 'top', 'backgroundColor': 'white', 'padding': '15px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'})
            ]),
            
            # Auto-refresh interval
            dcc.Interval(
                id='interval-component',
                interval=5*1000,  # Update every 5 seconds
                n_intervals=0
            )
        ], style={'backgroundColor': '#f8f9fa', 'padding': '20px', 'fontFamily': 'Arial, sans-serif'})
    
    def setup_callbacks(self):
        """
        Setup dashboard callbacks for interactivity
        """
        @self.app.callback([
            Output('stats-row', 'children'),
            Output('emotion-pie-chart', 'figure'),
            Output('emotion-timeline', 'figure'),
            Output('topic-emotion-heatmap', 'figure'),
            Output('live-tweets', 'children')
        ], [Input('interval-component', 'n_intervals')])
        def update_dashboard(n):
            # Get latest data for visualizations
            df = self.get_latest_data()
            
            # Get real-time cumulative stats
            real_time_stats = self.get_real_time_stats()
            
            if df.empty:
                empty_fig = go.Figure()
                empty_fig.add_annotation(text="No data available", showarrow=False, font=dict(size=20))
                return [], empty_fig, empty_fig, empty_fig, [html.P("No tweets available", style={'textAlign': 'center', 'color': '#95a5a6'})]
            
            # Stats Row (using real-time stats for totals)
            stats = self.create_stats_row(df, real_time_stats)
            
            # Emotion Pie Chart (using real-time stats for current batch distribution)
            emotion_pie = self.create_emotion_pie_chart(df, real_time_stats)
            
            # Emotion Timeline (using historical data)
            emotion_timeline = self.create_emotion_timeline(df)
            
            # Topic-Emotion Heatmap
            topic_heatmap = self.create_topic_emotion_heatmap(df)
            
            # Live Tweets
            live_tweets = self.create_live_tweets(df)
            
            return stats, emotion_pie, emotion_timeline, topic_heatmap, live_tweets
    
    def create_stats_row(self, df, real_time_stats):
        """
        Create statistics row with key metrics using real-time cumulative stats
        """
        # Use real-time stats for cumulative totals
        total_tweets = real_time_stats.get('total_tweets_processed', len(df))
        total_high_confidence = real_time_stats.get('total_high_confidence', 0)
        avg_confidence = real_time_stats.get('avg_emotion_confidence', df['emotion_confidence'].mean() if not df.empty else 0)
        
        # Get most common emotion from cumulative stats or fallback to df
        cumulative_emotion_dist = real_time_stats.get('cumulative_emotion_distribution', {})
        if cumulative_emotion_dist:
            most_common_emotion = max(cumulative_emotion_dist.keys(), key=cumulative_emotion_dist.get)
        else:
            emotion_dist = real_time_stats.get('current_batch_emotion_distribution', {})
            if emotion_dist:
                most_common_emotion = max(emotion_dist.keys(), key=emotion_dist.get)
            else:
                most_common_emotion = df['dominant_emotion'].mode().iloc[0] if not df.empty else "N/A"
        
        # Calculate positive ratio from cumulative stats or fallback to df
        cumulative_sentiment_dist = real_time_stats.get('cumulative_sentiment_distribution', {})
        if cumulative_sentiment_dist:
            total_sentiment = sum(cumulative_sentiment_dist.values())
            positive_ratio = (cumulative_sentiment_dist.get('positive', 0) / total_sentiment * 100) if total_sentiment > 0 else 0
        else:
            sentiment_dist = real_time_stats.get('current_batch_sentiment_distribution', {})
            if sentiment_dist:
                total_sentiment = sum(sentiment_dist.values())
                positive_ratio = (sentiment_dist.get('positive', 0) / total_sentiment * 100) if total_sentiment > 0 else 0
            else:
                positive_ratio = (df['sentiment_label'] == 'positive').mean() * 100 if not df.empty else 0
        
        stats = [
            html.Div([
                html.H2(f"{total_tweets:,}", style={'margin': '0', 'color': '#3498db', 'fontSize': '2em'}),
                html.P("🔢 Total Tweets", style={'margin': '0', 'fontSize': '14px', 'fontWeight': 'bold'})
            ], style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': '20px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)', 'width': '22%', 'display': 'inline-block', 'margin': '1%'}),
            
            html.Div([
                html.H2(f"{avg_confidence:.3f}", style={'margin': '0', 'color': '#2ecc71', 'fontSize': '2em'}),
                html.P("🎯 Avg Confidence", style={'margin': '0', 'fontSize': '14px', 'fontWeight': 'bold'})
            ], style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': '20px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)', 'width': '22%', 'display': 'inline-block', 'margin': '1%'}),
            
            html.Div([
                html.H2(most_common_emotion.title(), style={'margin': '0', 'color': '#e74c3c', 'fontSize': '1.8em'}),
                html.P("🏆 Top Emotion", style={'margin': '0', 'fontSize': '14px', 'fontWeight': 'bold'})
            ], style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': '20px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)', 'width': '22%', 'display': 'inline-block', 'margin': '1%'}),
            
            html.Div([
                html.H2(f"{total_high_confidence:,}", style={'margin': '0', 'color': '#f39c12', 'fontSize': '2em'}),
                html.P("⭐ High Confidence", style={'margin': '0', 'fontSize': '14px', 'fontWeight': 'bold'})
            ], style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': '20px', 'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)', 'width': '22%', 'display': 'inline-block', 'margin': '1%'})
        ]
        
        return stats
    
    def create_emotion_pie_chart(self, df, real_time_stats):
        """
        Create emotion distribution pie chart using cumulative data from all tweets
        """
        # First try to get cumulative distribution from real-time stats
        cumulative_emotion_dist = real_time_stats.get('cumulative_emotion_distribution', {})
        
        if cumulative_emotion_dist:
            emotion_counts = pd.Series(cumulative_emotion_dist)
        else:
            # Fallback: get emotion distribution from all tweets in the database
            try:
                pipeline = [
                    {"$group": {"_id": "$dominant_emotion", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                emotion_aggregation = list(self.collection.aggregate(pipeline))
                
                if emotion_aggregation:
                    emotions = [item['_id'] for item in emotion_aggregation if item['_id']]
                    counts = [item['count'] for item in emotion_aggregation if item['_id']]
                    emotion_counts = pd.Series(counts, index=emotions)
                else:
                    # Final fallback to current batch
                    current_batch_dist = real_time_stats.get('current_batch_emotion_distribution', {})
                    if current_batch_dist:
                        emotion_counts = pd.Series(current_batch_dist)
                    else:
                        emotion_counts = df['dominant_emotion'].value_counts()
            except Exception as e:
                print(f"Error getting cumulative emotion distribution: {e}")
                # Final fallback to dataframe
                emotion_counts = df['dominant_emotion'].value_counts()
        
        colors = {
            'joy': '#f1c40f', 'anger': '#e74c3c', 'fear': '#9b59b6',
            'sadness': '#3498db', 'disgust': '#95a5a6', 'surprise': '#e67e22',
            'trust': '#2ecc71', 'anticipation': '#1abc9c'
        }
        
        fig = go.Figure(data=[go.Pie(
            labels=emotion_counts.index,
            values=emotion_counts.values,
            marker=dict(colors=[colors.get(emotion, '#bdc3c7') for emotion in emotion_counts.index]),
            textinfo='label+percent',
            textfont_size=12
        )])
        
        fig.update_layout(
            title="",
            font=dict(size=12),
            showlegend=True,
            height=300,
            margin=dict(t=0, b=0, l=0, r=0)
        )
        
        return fig
    
    def create_emotion_timeline(self, df):
        """
        Create emotion timeline chart
        """
        # Group by 5-minute intervals
        df['time_group'] = df['timestamp'].dt.floor('5min')
        timeline_data = df.groupby(['time_group', 'dominant_emotion']).size().reset_index(name='count')
        
        fig = px.line(timeline_data, x='time_group', y='count', color='dominant_emotion',
                     title="", line_shape='spline')
        
        fig.update_layout(
            height=300,
            margin=dict(t=0, b=50, l=50, r=0),
            xaxis_title="Time",
            yaxis_title="Tweet Count",
            legend_title="Emotion"
        )
        
        return fig
    
    def create_topic_emotion_heatmap(self, df):
        """
        Create topic-emotion heatmap
        """
        heatmap_data = pd.crosstab(df['topic'], df['dominant_emotion'])
        
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data.values,
            x=heatmap_data.columns,
            y=heatmap_data.index,
            colorscale='Viridis',
            showscale=True
        ))
        
        fig.update_layout(
            title="",
            height=300,
            margin=dict(t=0, b=50, l=100, r=0),
            xaxis_title="Emotion",
            yaxis_title="Topic"
        )
        
        return fig
    
    def create_live_tweets(self, df):
        """
        Create live tweets feed
        """
        latest_tweets = df.head(10)
        
        tweets_html = []
        for _, row in latest_tweets.iterrows():
            # Emotion emoji mapping
            emotion_emojis = {
                'joy': '😊', 'anger': '😠', 'fear': '😨',
                'sadness': '😢', 'disgust': '🤢', 'surprise': '😲',
                'trust': '🤝', 'anticipation': '🤔'
            }
            
            emoji = emotion_emojis.get(row['dominant_emotion'], '😐')
            
            tweet_html = html.Div([
                html.Div([
                    html.Span(f"{emoji} {row['dominant_emotion'].upper()}", 
                             style={'backgroundColor': '#3498db', 'color': 'white', 'padding': '2px 8px', 'borderRadius': '12px', 'fontSize': '12px', 'fontWeight': 'bold'}),
                    html.Span(f"{row['emotion_confidence']:.2f}", 
                             style={'backgroundColor': '#2ecc71', 'color': 'white', 'padding': '2px 6px', 'borderRadius': '8px', 'fontSize': '10px', 'marginLeft': '5px'})
                ], style={'marginBottom': '5px'}),
                
                html.P(row['original_text'][:120] + "..." if len(row['original_text']) > 120 else row['original_text'],
                      style={'margin': '5px 0', 'fontSize': '14px', 'lineHeight': '1.4'}),
                
                html.Div([
                    html.Span(f"📍 {row['location']}", style={'fontSize': '12px', 'color': '#7f8c8d', 'marginRight': '10px'}),
                    html.Span(f"🏷️ {row['topic']}", style={'fontSize': '12px', 'color': '#7f8c8d', 'marginRight': '10px'}),
                    html.Span(f"⏰ {row['timestamp'].strftime('%H:%M:%S')}", style={'fontSize': '12px', 'color': '#7f8c8d'})
                ])
            ], style={'padding': '10px', 'marginBottom': '10px', 'backgroundColor': '#f8f9fa', 'borderRadius': '8px', 'border': '1px solid #dee2e6'})
            
            tweets_html.append(tweet_html)
        
        return tweets_html
    
    def run(self, debug=False, port=8050):
        """
        Run the dashboard
        """
        print("🚀 Starting Twitter Emotion Dashboard...")
        print(f"📊 Dashboard available at: http://localhost:{port}")
        print("🎭 Features: Real-time emotion detection, live feed, interactive charts")
        self.app.run(debug=debug, port=port, host='0.0.0.0')

if __name__ == "__main__":
    dashboard = EmotionDashboard()
    dashboard.run(debug=True)