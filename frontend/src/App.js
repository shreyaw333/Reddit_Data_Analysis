import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, ResponsiveContainer, BarChart, Bar, PieChart, Pie, Cell } from 'recharts';

function App() {
  const [dashboardData, setDashboardData] = useState({
    totalPosts: 47200,
    activeSubreddits: 156,
    avgEngagement: 342,
    weeklyGrowth: 12
  });

  const trendingPosts = [
    {
      title: "AI breakthrough: New language model achieves human-level reasoning",
      subreddit: "r/technology",
      upvotes: "2.4k",
      comments: "847",
      sentiment: "92% positive"
    },
    {
      title: "Python 3.12 released with major performance improvements",
      subreddit: "r/programming",
      upvotes: "1.8k",
      comments: "312",
      sentiment: "89% positive"
    },
    {
      title: "Data science job market analysis for 2024",
      subreddit: "r/datascience",
      upvotes: "1.2k",
      comments: "156",
      sentiment: "74% positive"
    }
  ];

  const subredditPerformance = [
    { name: "r/technology", posts: 1200, comments: 15700, avgScore: 485 },
    { name: "r/programming", posts: 856, comments: 9200, avgScore: 312 },
    { name: "r/datascience", posts: 623, comments: 5800, avgScore: 298 },
    { name: "r/MachineLearning", posts: 445, comments: 3100, avgScore: 267 }
  ];

  const sentimentData = [
    { name: "Very Positive", value: 2900, color: "#ea580c" },
    { name: "Positive", value: 5600, color: "#f97316" },
    { name: "Neutral", value: 3200, color: "#fb923c" },
    { name: "Negative", value: 1100, color: "#fdba74" },
    { name: "Very Negative", value: 600, color: "#fed7aa" }
  ];

  const activityHeatmap = [
    { time: "12AM", activity: 45 },
    { time: "6AM", activity: 25 },
    { time: "12PM", activity: 65 },
    { time: "6PM", activity: 87 },
    { time: "11PM", activity: 78 }
  ];

  const volumeData = [
    { day: "Mon", posts: 1800, comments: 8500 },
    { day: "Tue", posts: 2100, comments: 12500 },
    { day: "Wed", posts: 1900, comments: 10200 },
    { day: "Thu", posts: 2300, comments: 14800 },
    { day: "Fri", posts: 2000, comments: 12800 },
    { day: "Sat", posts: 2400, comments: 16500 },
    { day: "Sun", posts: 2200, comments: 15200 }
  ];

  const pieChartData = [
    { name: "r/technology", value: 35, color: "#ea580c" },
    { name: "r/programming", value: 25, color: "#f97316" },
    { name: "r/datascience", value: 18, color: "#fb923c" },
    { name: "r/MachineLearning", value: 12, color: "#fdba74" },
    { name: "Others", value: 10, color: "#fed7aa" }
  ];

  const cardStyle = {
    background: 'linear-gradient(135deg, #f97316 0%, #ea580c 100%)',
    borderRadius: '12px',
    padding: '24px',
    color: 'white',
    boxShadow: '0 10px 25px rgba(0, 0, 0, 0.1)',
    marginBottom: '16px'
  };

  const containerStyle = {
    minHeight: '100vh',
    backgroundColor: '#f9fafb',
    padding: '24px',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
  };

  const whiteCardStyle = {
    backgroundColor: 'white',
    borderRadius: '12px',
    padding: '24px',
    boxShadow: '0 4px 6px rgba(0, 0, 0, 0.05)',
    border: '1px solid #e5e7eb',
    marginBottom: '32px'
  };

  const sectionHeaderStyle = {
    display: 'flex',
    alignItems: 'center',
    marginBottom: '24px'
  };

  const colorBarStyle = {
    width: '8px',
    height: '24px',
    borderRadius: '4px',
    marginRight: '12px'
  };

  return (
    <div style={containerStyle}>
      <div style={{ maxWidth: '1280px', margin: '0 auto' }}>
        {/* Header */}
        <div style={{ marginBottom: '32px' }}>
          <h1 style={{ fontSize: '36px', fontWeight: 'bold', color: '#111827', marginBottom: '8px' }}>
            Reddit Data Pipeline Dashboard
          </h1>
          <p style={{ color: '#6b7280', fontSize: '16px' }}>
            Real-time analytics from your Reddit data pipeline
          </p>
        </div>

        {/* KPI Cards */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '24px', marginBottom: '32px' }}>
          <div style={cardStyle}>
            <h3 style={{ fontSize: '18px', fontWeight: '500', opacity: 0.9, marginBottom: '8px' }}>
              Total Posts Analyzed
            </h3>
            <div style={{ fontSize: '48px', fontWeight: 'bold', marginBottom: '4px' }}>47.2K</div>
            <div style={{ fontSize: '14px', opacity: 0.8 }}>+12% this week</div>
          </div>
          <div style={cardStyle}>
            <h3 style={{ fontSize: '18px', fontWeight: '500', opacity: 0.9, marginBottom: '8px' }}>
              Active Subreddits
            </h3>
            <div style={{ fontSize: '48px', fontWeight: 'bold', marginBottom: '4px' }}>156</div>
            <div style={{ fontSize: '14px', opacity: 0.8 }}>Monitoring daily</div>
          </div>
          <div style={cardStyle}>
            <h3 style={{ fontSize: '18px', fontWeight: '500', opacity: 0.9, marginBottom: '8px' }}>
              Avg Engagement
            </h3>
            <div style={{ fontSize: '48px', fontWeight: 'bold', marginBottom: '4px' }}>342</div>
            <div style={{ fontSize: '14px', opacity: 0.8 }}>Comments + Upvotes</div>
          </div>
        </div>

        {/* Main Content Grid */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(500px, 1fr))', gap: '32px', marginBottom: '32px' }}>
          {/* Trending Posts */}
          <div style={whiteCardStyle}>
            <div style={sectionHeaderStyle}>
              <div style={{ ...colorBarStyle, backgroundColor: '#f97316' }}></div>
              <h2 style={{ fontSize: '20px', fontWeight: '600', color: '#111827' }}>
                Trending Posts (Last 24h)
              </h2>
            </div>
            <div>
              {trendingPosts.map((post, index) => (
                <div key={index} style={{ 
                  borderBottom: index !== trendingPosts.length - 1 ? '1px solid #f3f4f6' : 'none',
                  paddingBottom: '16px',
                  marginBottom: '16px'
                }}>
                  <h3 style={{ fontWeight: '500', color: '#111827', marginBottom: '8px', lineHeight: '1.4' }}>
                    {post.title}
                  </h3>
                  <div style={{ display: 'flex', alignItems: 'center', fontSize: '14px', color: '#6b7280', gap: '16px', flexWrap: 'wrap' }}>
                    <span>{post.subreddit}</span>
                    <span>{post.upvotes} upvotes</span>
                    <span>{post.comments} comments</span>
                    <span style={{ color: '#059669' }}>{post.sentiment}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Sentiment Analysis */}
          <div style={whiteCardStyle}>
            <div style={sectionHeaderStyle}>
              <div style={{ ...colorBarStyle, backgroundColor: '#f97316' }}></div>
              <h2 style={{ fontSize: '20px', fontWeight: '600', color: '#111827' }}>
                Sentiment Analysis Trends
              </h2>
            </div>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={sentimentData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis dataKey="name" tick={{fontSize: 12}} />
                <YAxis tick={{fontSize: 12}} />
                <Bar dataKey="value" fill="#f97316" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
            <div style={{ fontSize: '12px', color: '#9ca3af', marginTop: '8px' }}>
              Sentiment Distribution
            </div>
          </div>
        </div>

        {/* Second Row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(500px, 1fr))', gap: '32px', marginBottom: '32px' }}>
          {/* Subreddit Performance */}
          <div style={whiteCardStyle}>
            <div style={sectionHeaderStyle}>
              <div style={{ ...colorBarStyle, backgroundColor: '#f97316' }}></div>
              <h2 style={{ fontSize: '20px', fontWeight: '600', color: '#111827' }}>
                Subreddit Performance
              </h2>
            </div>
            <div>
              {subredditPerformance.map((subreddit, index) => (
                <div key={index} style={{ 
                  display: 'flex', 
                  alignItems: 'center', 
                  justifyContent: 'space-between', 
                  padding: '12px 0',
                  borderBottom: index !== subredditPerformance.length - 1 ? '1px solid #f3f4f6' : 'none'
                }}>
                  <div style={{ fontWeight: '500', color: '#111827' }}>{subreddit.name}</div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '24px', fontSize: '14px', color: '#6b7280' }}>
                    <span>{subreddit.posts} posts</span>
                    <span>{subreddit.comments.toLocaleString()} comments</span>
                    <span>Avg: {subreddit.avgScore} score</span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Activity Heatmap */}
          <div style={whiteCardStyle}>
            <div style={sectionHeaderStyle}>
              <div style={{ ...colorBarStyle, backgroundColor: '#f97316' }}></div>
              <h2 style={{ fontSize: '20px', fontWeight: '600', color: '#111827' }}>
                Activity Heatmap
              </h2>
            </div>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={activityHeatmap}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis dataKey="time" tick={{fontSize: 12}} />
                <YAxis tick={{fontSize: 12}} />
                <Bar dataKey="activity" fill="#ea580c" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
            <div style={{ fontSize: '12px', color: '#9ca3af', marginTop: '8px' }}>
              Activity Level
            </div>
          </div>
        </div>

        {/* Bottom Row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(500px, 1fr))', gap: '32px' }}>
          {/* Post Volume Over Time */}
          <div style={whiteCardStyle}>
            <div style={sectionHeaderStyle}>
              <div style={{ ...colorBarStyle, backgroundColor: '#f97316' }}></div>
              <h2 style={{ fontSize: '20px', fontWeight: '600', color: '#111827' }}>
                Post Volume Over Time
              </h2>
            </div>
            <ResponsiveContainer width="100%" height={250}>
              <LineChart data={volumeData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis dataKey="day" tick={{fontSize: 12}} />
                <YAxis tick={{fontSize: 12}} />
                <Line 
                  type="monotone" 
                  dataKey="posts" 
                  stroke="#ea580c" 
                  strokeWidth={3}
                  dot={{fill: '#ea580c', strokeWidth: 2, r: 4}}
                />
                <Line 
                  type="monotone" 
                  dataKey="comments" 
                  stroke="#f97316" 
                  strokeWidth={3}
                  dot={{fill: '#f97316', strokeWidth: 2, r: 4}}
                />
              </LineChart>
            </ResponsiveContainer>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '24px', marginTop: '16px', fontSize: '14px' }}>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <div style={{ width: '12px', height: '12px', backgroundColor: '#ea580c', borderRadius: '2px', marginRight: '8px' }}></div>
                <span>Posts</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <div style={{ width: '12px', height: '12px', backgroundColor: '#f97316', borderRadius: '2px', marginRight: '8px' }}></div>
                <span>Comments</span>
              </div>
            </div>
          </div>

          {/* Top Performing Subreddits */}
          <div style={whiteCardStyle}>
            <div style={sectionHeaderStyle}>
              <div style={{ ...colorBarStyle, backgroundColor: '#f97316' }}></div>
              <h2 style={{ fontSize: '20px', fontWeight: '600', color: '#111827' }}>
                Top Performing Subreddits
              </h2>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={pieChartData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={100}
                    paddingAngle={2}
                    dataKey="value"
                  >
                    {pieChartData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '8px', marginTop: '16px' }}>
              {pieChartData.map((item, index) => (
                <div key={index} style={{ display: 'flex', alignItems: 'center', fontSize: '14px' }}>
                  <div 
                    style={{ 
                      width: '12px', 
                      height: '12px', 
                      borderRadius: '2px', 
                      marginRight: '8px',
                      backgroundColor: item.color
                    }}
                  ></div>
                  <span>{item.name}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;