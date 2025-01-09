# monitor.py
import os
import time
import signal
import pymongo
from datetime import datetime
import pandas as pd
import plotly.express as px
from flask import Flask, render_template_string, jsonify
import threading
import sys
import shutil

app = Flask(__name__)

MONGO_HOST = os.getenv('DBHOST', 'dbstorage')
DB_NAME = 'cloneDetector'
COLLECTIONS = ['files', 'chunks', 'candidates', 'clones', 'statusUpdates', 'statistics', 'processCompleted']
ARCHIVE_DIR = 'archives'
running = True
LOG_INTERVAL = float(os.getenv('LOG_INTERVAL', 1))

# Create archives directory if it doesn't exist
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def signal_handler(signum, frame):
    global running
    running = False
    print("Shutting down monitor...")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def connect_db():
    client = pymongo.MongoClient(f"mongodb://{MONGO_HOST}:27017/")
    return client[DB_NAME]

def clear_database():
    try:
        db = connect_db()
        for collection in COLLECTIONS:
            db[collection].drop()
        print(f"Cleared all collections in {DB_NAME} database:")
        print(f"Dropped collections: {', '.join(COLLECTIONS)}")
    except Exception as e:
        print(f"Error clearing database: {e}")
        sys.exit(1)

def save_final_report():
    try:
        with app.app_context():
            # Get the current page content
            current_page = index()
            
            # Save with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'clone_detector_report_{timestamp}.html'
            filepath = os.path.join(ARCHIVE_DIR, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(current_page)
            
            print(f"Final report saved to {filepath}")
            
    except Exception as e:
        print(f"Error saving final report: {e}")

def check_process_completed():
    db = connect_db()
    if db.processCompleted.count_documents({}) > 0:
        print("Clone detector process completed. Saving final report and shutting down...")
        save_final_report()
        db.processCompleted.drop()
        return True
    return False

def collect_stats():
    db = connect_db()
    
    while running:
        try:
            if check_process_completed():
                os._exit(0)

            counts = {
                'timestamp': datetime.now(),
                'files': db.files.count_documents({}),
                'chunks': db.chunks.count_documents({}),
                'candidates': db.candidates.count_documents({}),
                'clones': db.clones.count_documents({})
            }
            
            previous_stats = db.statistics.find_one(
                sort=[('timestamp', pymongo.DESCENDING)]
            )
            
            if previous_stats:
                time_diff = (counts['timestamp'] - previous_stats['timestamp']).total_seconds()
                for collection in ['chunks', 'candidates', 'clones']:
                    count_diff = counts[collection] - previous_stats[collection]
                    if time_diff > 0:
                        counts[f'{collection}_rate'] = count_diff / time_diff
                    else:
                        counts[f'{collection}_rate'] = 0
            else:
                # Initialize rates to 0 for first entry
                for collection in ['chunks', 'candidates', 'clones']:
                    counts[f'{collection}_rate'] = 0
            
            db.statistics.insert_one(counts)
            time.sleep(LOG_INTERVAL)  # Sample every LOG_INTERVAL seconds
            
        except pymongo.errors.ServerSelectionTimeoutError:
            print("Database connection lost. Shutting down...")
            os._exit(1)
        except Exception as e:
            print(f"Error in collect_stats: {e}")
            time.sleep(LOG_INTERVAL)

def create_plots(df, title_suffix=""):
    if not df.empty:
        fig_counts = px.line(df, x='timestamp', 
                            y=['files', 'chunks', 'candidates', 'clones'],
                            title=f'Collection Counts Over Time {title_suffix}')
        
        fig_rates = px.line(df, x='timestamp', 
                            y=['chunks_rate', 'candidates_rate', 'clones_rate'],
                            title=f'Processing Rates Over Time {title_suffix}')
        
        for fig in [fig_counts, fig_rates]:
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Count" if fig == fig_counts else "Items/second",
                hovermode='x unified',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            fig.update_xaxes(range=[df['timestamp'].min(), df['timestamp'].max()])
    else:
        fig_counts = px.line(title=f'No data yet {title_suffix}')
        fig_rates = px.line(title=f'No data yet {title_suffix}')
    
    return fig_counts, fig_rates

def get_stats_and_plots(db):
    """Common function to fetch statistics and generate plots"""
    all_stats = list(db.statistics.find().sort('timestamp', 1))
    recent_stats = list(db.statistics.find().sort('timestamp', -1).limit(100))
    recent_stats.reverse()
    
    df_all = pd.DataFrame(all_stats) if all_stats else pd.DataFrame()
    df_recent = pd.DataFrame(recent_stats) if recent_stats else pd.DataFrame()
    
    recent_counts, recent_rates = create_plots(df_recent, "(Recent)")
    all_counts, all_rates = create_plots(df_all, "(Complete)")
    
    updates = list(db.statusUpdates.find().sort('timestamp', -1))
    updates_html = ''
    for update in updates:
        timestamp = update['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        updates_html += f'<li class="update-item">{timestamp}: {update["message"]}</li>'
        
    return {
        'all_stats': all_stats,
        'recent_counts': recent_counts,
        'recent_rates': recent_rates,
        'all_counts': all_counts,
        'all_rates': all_rates,
        'updates_html': updates_html
    }

@app.route('/')
def index():
    try:
        db = connect_db()
        data = get_stats_and_plots(db)
        
        return render_template_string(
            HTML_TEMPLATE,
            all_stats=data['all_stats'],
            recent_counts_plot=data['recent_counts'].to_html(full_html=False, include_plotlyjs=False),
            recent_rates_plot=data['recent_rates'].to_html(full_html=False, include_plotlyjs=False),
            all_counts_plot=data['all_counts'].to_html(full_html=False, include_plotlyjs=False),
            all_rates_plot=data['all_rates'].to_html(full_html=False, include_plotlyjs=False),
            updates_html=data['updates_html']
        )
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/get_plots')
def get_plots():
    try:
        db = connect_db()
        data = get_stats_and_plots(db)
        
        return jsonify({
            'recent_counts': data['recent_counts'].to_json(),
            'recent_rates': data['recent_rates'].to_json(),
            'all_counts': data['all_counts'].to_json(),
            'all_rates': data['all_rates'].to_json(),
            'updates_html': data['updates_html']
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Clone Detector Monitor</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        .plot-container {
            margin: 20px 0;
            background-color: white;
            padding: 15px;
            border-radius: 4px;
            width: 100%;  /* Ensure plot container takes full width */
        }
        .js-plotly-plot {
            width: 100% !important;  /* Force plot width */
        }
        .plotly {
            width: 100% !important;  /* Force plotly container width */
        }
    </style>
    <script>
        let updateTimer;
        
        function updatePlots() {
            fetch('/get_plots')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('Error fetching plots:', data.error);
                        return;
                    }
                    
                    // Update status updates
                    document.getElementById('status-updates').innerHTML = data.updates_html;
                    
                    // Update plots
                    const plotData = {
                        'recent-counts': JSON.parse(data.recent_counts),
                        'recent-rates': JSON.parse(data.recent_rates),
                        'all-counts': JSON.parse(data.all_counts),
                        'all-rates': JSON.parse(data.all_rates)
                    };
                    
                    for (const [elementId, plotJson] of Object.entries(plotData)) {
                        const element = document.getElementById(elementId);
                        if (element) {
                            Plotly.react(elementId, plotJson.data, plotJson.layout);
                        }
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                });
        }
        
        function startAutoUpdate() {
            updateTimer = setInterval(updatePlots, 5000); // Update every 10 seconds
        }

        function stopAutoUpdate() {
            clearInterval(updateTimer);
        }
        
        window.onload = function() {
            startAutoUpdate();
            
            // Handle visibility change to pause updates when tab is not visible
            document.addEventListener('visibilitychange', function() {
                if (document.hidden) {
                    stopAutoUpdate();
                } else {
                    updatePlots(); // Immediate update when becoming visible
                    startAutoUpdate();
                }
            });
        }
        
        window.addEventListener('resize', function() {
            var plots = document.getElementsByClassName('js-plotly-plot');
            for(var i = 0; i < plots.length; i++) {
                Plotly.Plots.resize(plots[i]);
            }
        });
    </script>
</head>
<body>
    <div class="container">
        <h1>Clone Detector Monitor</h1>        
        
        <div class="plot-view recent">
            <div class="plot-container">
                <div id="recent-counts">{{recent_counts_plot | safe}}</div>
            </div>
            <div class="plot-container">
                <div id="recent-rates">{{recent_rates_plot | safe}}</div>
            </div>
        </div>
        
        <div class="plot-view complete">
            <div class="plot-container">
                <div id="all-counts">{{all_counts_plot | safe}}</div>
            </div>
            <div class="plot-container">
                <div id="all-rates">{{all_rates_plot | safe}}</div>
            </div>
        </div>
        
        <div class="updates">
            <h2>Recent Status Updates</h2>
            <ul id="status-updates" style="list-style-type: none; padding: 0;">
                {{updates_html | safe}}
            </ul>
        </div>
    </div>
</body>
</html>
'''

def wait_for_database():
    max_attempts = 1000
    attempt = 0
    while attempt < max_attempts:
        try:
            db = connect_db()
            # Test connection with a simple command
            db.command('ping')
            print("Successfully connected to database")
            return True
        except Exception as e:
            attempt += 1
            print(f"Waiting for database... Attempt {attempt}/{max_attempts}")
            time.sleep(0.01)
    
    print("Failed to connect to database after maximum attempts")
    return False

if __name__ == '__main__':
    # Wait for database to be ready
    if not wait_for_database():
        sys.exit(1)
    clear_database()

    stats_thread = threading.Thread(target=collect_stats)
    stats_thread.daemon = True
    stats_thread.start()
    app.run(host='0.0.0.0', port=8080)