import pandas as pd
import plotly.express as px
import streamlit as st
import asyncio
from datetime import datetime, timedelta

async def get_daily_metrics(conn, start_date, end_date):
    """Get daily chat metrics aggregated from chat_interactions."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as total_chats,
                COUNT(DISTINCT user_id) as unique_users,
                AVG(response_time) as avg_response_time,
                SUM(CASE WHEN error_occurred THEN 1 ELSE 0 END)::float / COUNT(*) as error_rate,
                AVG(navigation_matches + publication_matches) as avg_matches
            FROM chat_interactions 
            WHERE timestamp BETWEEN %s AND %s
            GROUP BY DATE(timestamp)
            ORDER BY date
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

async def get_intent_performance(conn, start_date, end_date):
    """Get metrics from intent_performance_metrics view."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                intent_type,
                COUNT(*) as total_queries,
                AVG(intent_confidence) as avg_confidence,
                AVG(response_time) as avg_response_time,
                SUM(CASE WHEN error_occurred THEN 1 ELSE 0 END)::float / COUNT(*) as error_rate
            FROM chat_interactions
            WHERE timestamp BETWEEN %s AND %s
                AND intent_type IS NOT NULL
            GROUP BY intent_type
            ORDER BY total_queries DESC
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

async def get_sentiment_trends(conn, start_date, end_date):
    """Get sentiment metrics from sentiment_metrics table."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                DATE(ci.timestamp) as date,
                AVG(sm.sentiment_score) as avg_sentiment,
                AVG(sm.satisfaction_score) as avg_satisfaction,
                AVG(sm.urgency_score) as avg_urgency,
                AVG(sm.clarity_score) as avg_clarity,
                COUNT(*) as total_interactions
            FROM chat_interactions ci
            JOIN sentiment_metrics sm ON ci.id = sm.interaction_id
            WHERE ci.timestamp BETWEEN %s AND %s
            GROUP BY DATE(ci.timestamp)
            ORDER BY date
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

async def get_content_matching_stats(conn, start_date, end_date):
    """Get content matching metrics from chat_analytics table."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                content_type,
                COUNT(*) as total_matches,
                AVG(similarity_score) as avg_similarity,
                AVG(rank_position) as avg_rank
            FROM chat_analytics ca
            JOIN chat_interactions ci ON ca.interaction_id = ci.id
            WHERE ci.timestamp BETWEEN %s AND %s
            GROUP BY content_type
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

def get_chat_metrics(conn, start_date, end_date):
    """
    Synchronous wrapper for getting chat metrics.
    """
    async def get_metrics_async():
        # Gather all metrics concurrently
        daily_metrics, intent_metrics, sentiment_metrics, content_metrics = await asyncio.gather(
            get_daily_metrics(conn, start_date, end_date),
            get_intent_performance(conn, start_date, end_date),
            get_sentiment_trends(conn, start_date, end_date),
            get_content_matching_stats(conn, start_date, end_date)
        )
        
        # Merge daily and sentiment metrics on date
        metrics = pd.merge(
            daily_metrics,
            sentiment_metrics,
            on='date',
            how='left'
        )
        
        # Store intent and content metrics as additional data
        metrics.attrs['intent_metrics'] = intent_metrics
        metrics.attrs['content_metrics'] = content_metrics
        
        return metrics

    # Run async code in sync context
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        metrics = loop.run_until_complete(get_metrics_async())
        loop.close()
        return metrics
    except Exception as e:
        st.error(f"Error getting chat metrics: {e}")
        raise

def display_chat_analytics(metrics_df, filters):
    """Display comprehensive chat analytics dashboard"""
    st.title("Chat Analytics Dashboard")
    
    try:
        if not isinstance(metrics_df, pd.DataFrame):
            st.error("Invalid metrics data received.")
            return
            
        if metrics_df.empty:
            st.warning("No data available for the selected date range.")
            return
        
        # Overview metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_chats = metrics_df['total_chats'].sum()
            st.metric("Total Chats", f"{total_chats:,}")
        
        with col2:
            avg_response = metrics_df['avg_response_time'].mean()
            st.metric("Avg Response Time", f"{avg_response:.2f}s")
        
        with col3:
            avg_sentiment = metrics_df['avg_sentiment'].mean()
            st.metric("Avg Sentiment", f"{avg_sentiment:.2f}")
        
        with col4:
            avg_matches = metrics_df['avg_matches'].mean()
            st.metric("Avg Matches", f"{avg_matches:.1f}")

        # Chat Volume Trends
        st.subheader("Chat Volume Trends")
        volume_fig = px.line(metrics_df, 
                            x='date', 
                            y=['total_chats', 'unique_users'],
                            title="Daily Chat Volume")
        st.plotly_chart(volume_fig)

        # Response Time and Error Rate
        st.subheader("Performance Metrics")
        perf_fig = px.line(metrics_df,
                        x='date',
                        y=['avg_response_time', 'error_rate'],
                        title="Response Time and Error Rate")
        st.plotly_chart(perf_fig)

        # Sentiment Analysis
        if 'avg_sentiment' in metrics_df.columns:
            st.subheader("Sentiment Analysis")
            sentiment_fig = px.line(metrics_df,
                                x='date',
                                y=['avg_sentiment', 'avg_satisfaction', 'avg_clarity'],
                                title="Sentiment Trends")
            st.plotly_chart(sentiment_fig)
        
        # Intent Analysis
        if st.checkbox("Show Intent Analysis"):
            st.subheader("Intent Performance")
            intent_df = metrics_df.attrs.get('intent_metrics')
            if intent_df is not None and not intent_df.empty:
                st.dataframe(
                    intent_df.style.format({
                        'avg_confidence': '{:.2%}',
                        'avg_response_time': '{:.2f}s',
                        'error_rate': '{:.2%}'
                    })
                )

        # Content Matching Analysis
        if st.checkbox("Show Content Matching Analysis"):
            st.subheader("Content Matching Performance")
            content_df = metrics_df.attrs.get('content_metrics')
            if content_df is not None and not content_df.empty:
                st.dataframe(
                    content_df.style.format({
                        'avg_similarity': '{:.2%}',
                        'avg_rank': '{:.1f}'
                    })
                )

        # Add date/time of last update
        st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        
    except Exception as e:
        st.error(f"Error displaying chat analytics: {e}")
        raise