import sys
import os 
import io
import csv
from flask import Flask, render_template, request, flash, Response, redirect, url_for
import psycopg2 
from psycopg2.extras import DictCursor
from urllib.parse import urlparse 

# --- Configuration ---
app = Flask(__name__)
# Use an environment variable for the secret key
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'default_fallback_secret_key') 

# --- Database Utility Functions ---

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    database_url = os.environ.get('postgresql://santhosh:ZgvRvAqP6PYyMaB6U7p3Lg1XAU7RwPS1@dpg-d4qolb2li9vc73a1inhg-a.virginia-postgres.render.com/masterhc')
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is not set.", file=sys.stderr)
        return None
    try:
        conn = psycopg2.connect(database_url)
        print("INFO: PostgreSQL connection established successfully.")
        return conn
    except psycopg2.Error as err:
        print(f"FATAL: Error connecting to PostgreSQL. Error: {err}", file=sys.stderr)
        return None 

def populate_site_data(conn):
    """Inserts sample data into the site_data table."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM site_data")
    if cursor.fetchone()[0] > 0:
        print("INFO: site_data table already contains data. Skipping population.")
        cursor.close()
        return

    data = [
        ('APAC', 'Hub A', 'India', 'Site 1', 50, 60, 40), 
        ('APAC', 'Hub A', 'India', 'Site 2', 100, 50, 50), 
        ('EMEA', 'Hub B', 'Germany', 'Site 3', 30, 40, 30), 
        ('EMEA', 'Hub B', 'UK', 'Site 4', 150, 100, 50), 
        ('AMER', 'Hub C', 'USA', 'Site 5', 200, 150, 100), 
        ('AMER', 'Hub C', 'Canada', 'Site 6', 10, 20, 20), 
        ('APAC', 'Hub D', 'Japan', 'Site 7', 60, 30, 30), 
        ('AMER', 'Hub C', 'USA', 'Site 8', 50, 50, 50), 
    ]
    insert_query = """
        INSERT INTO site_data (region, hub, country, site, rse_count, dse_count, itc_count) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.executemany(insert_query, data)
        conn.commit()
        print("INFO: site_data table populated successfully.")
    except psycopg2.Error as err:
        print(f"ERROR inserting sample data: {err}", file=sys.stderr)
    finally:
        cursor.close()

def update_site_data(row_id, rse_count, dse_count, itc_count):
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    cursor = conn.cursor()
    update_query = """
        UPDATE site_data
        SET rse_count = %s, dse_count = %s, itc_count = %s
        WHERE id = %s
    """
    params = (rse_count, dse_count, itc_count, row_id)
    try:
        cursor.execute(update_query, params)
        conn.commit()
        return True, "Data updated successfully."
    except psycopg2.Error as err:
        conn.rollback()
        return False, f"Database update failed: {err}"
    finally:
        cursor.close()
        conn.close()

def get_site_details(row_id):
    conn = get_db_connection()
    if conn is None:
        return None
    cursor = conn.cursor(cursor_factory=DictCursor)
    query = "SELECT region, hub, country, site FROM site_data WHERE id = %s" 
    try:
        cursor.execute(query, (row_id,))
        return cursor.fetchone()
    except psycopg2.Error as err:
        print(f"Error fetching site details: {err}", file=sys.stderr)
        return None
    finally:
        cursor.close()
        conn.close()

def init_db():
    conn = get_db_connection()
    if conn is not None:
        cursor = conn.cursor()
        try:
            # PostgreSQL syntax: SERIAL PRIMARY KEY
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS site_data (
                    id SERIAL PRIMARY KEY,
                    region VARCHAR(50) NOT NULL,
                    hub VARCHAR(50) NOT NULL,
                    country VARCHAR(50) NOT NULL,
                    site VARCHAR(50) NOT NULL,
                    rse_count INT NOT NULL,
                    dse_count INT NOT NULL,
                    itc_count INT NOT NULL
                )
            ''')
            conn.commit()
            print("INFO: site_data table ensured to exist using PostgreSQL syntax.")
            populate_site_data(conn)
        except psycopg2.Error as err:
            print(f"ERROR during database initialization: {err}", file=sys.stderr)
        finally:
            cursor.close()
            conn.close()
    else:
        print("CRITICAL: Failed to connect to PostgreSQL during initialization. Application will likely fail on data routes.", file=sys.stderr)


# --- Routes ---

@app.route('/', methods=['GET', 'POST'])
def index():
    conn = get_db_connection()
    if conn is None:
        flash('Database connection failed. Check server status and configuration.', 'danger')
        return render_template('index.html', regions=[], hubs=[], countries=[], sites=[], filter_data=None)
        
    cursor = conn.cursor(cursor_factory=DictCursor)
    
    try:
        def fetch_unique_values(column):
            cursor.execute(f"SELECT DISTINCT {column} FROM site_data ORDER BY {column}")
            return [row[column] for row in cursor.fetchall()]

        regions = fetch_unique_values('region')
        hubs = fetch_unique_values('hub')
        countries = fetch_unique_values('country')
        sites = fetch_unique_values('site')
    except psycopg2.Error as err:
        flash(f"Error fetching dropdown data: {err}", 'danger')
        regions, hubs, countries, sites = [], [], [], [] 
    
    filter_data = None
    
    if request.method == 'POST': 
        selected_region = request.form.get('region_filter', 'All') 
        selected_hub = request.form.get('hub_filter', 'All')
        selected_country = request.form.get('country_filter', 'All')
        selected_site = request.form.get('site_filter', 'All')
        
        conditions = []
        params = []
        
        if selected_region and selected_region != 'All':
            conditions.append("region = %s")
            params.append(selected_region)
        if selected_hub and selected_hub != 'All':
            conditions.append("hub = %s")
            params.append(selected_hub)
        if selected_country and selected_country != 'All':
            conditions.append("country = %s")
            params.append(selected_country)
        if selected_site and selected_site != 'All':
            conditions.append("site = %s")
            params.append(selected_site)
            
        where_clause = " AND ".join(conditions)
        
        query = "SELECT id, region, hub, country, site, rse_count, dse_count, itc_count FROM site_data"
        if where_clause:
            query += " WHERE " + where_clause
            
        query += " ORDER BY region, hub, country, site"
            
        try:
            cursor.execute(query, tuple(params))
            filtered_rows = cursor.fetchall()
            total_associates = sum(
                row['rse_count'] + row['dse_count'] + row['itc_count'] 
                for row in filtered_rows
            )
            filter_data = {
                'region': selected_region,
                'hub': selected_hub,
                'country': selected_country,
                'site': selected_site,
                'total_count': total_associates,
                'filtered_rows': filtered_rows
            }
        except psycopg2.Error as err:
            flash(f"Error executing filter query: {err}", 'danger')
            
    cursor.close()
    conn.close()

    return render_template(
        'index.html',
        regions=regions, 
        hubs=hubs, 
        countries=countries, 
        sites=sites, 
        filter_data=filter_data
    )

@app.route('/edit_data/<int:row_id>', methods=['POST'])
def edit_data(row_id):
    location_data = get_site_details(row_id)
    if not location_data:
        flash(f"Error: Could not find record ID {row_id} for update.", 'danger')
        return redirect(url_for('index'))
    try:
        new_rse = int(request.form.get('rse_count'))
        new_dse = int(request.form.get('dse_count'))
        new_itc = int(request.form.get('itc_count'))
    except (ValueError, TypeError):
        flash('Invalid count value. Counts must be integers.', 'danger')
        return redirect(url_for('index'))
    success, message = update_site_data(row_id, new_rse, new_dse, new_itc)
    if success:
        flash(f"Successfully updated data for {location_data['region']} - {location_data['site']}. New RSE: {new_rse}, DSE: {new_dse}, ITC: {new_itc}.", 'success')
    else:
        flash(f"Update failed: {message}", 'danger')
    return redirect(url_for('index'))


@app.route('/download_data', methods=['POST'])
def download_data():
    conn = get_db_connection()
    if conn is None:
        flash('Database connection failed. Cannot download data.', 'danger')
        return redirect(url_for('index'))

    cursor = conn.cursor(cursor_factory=DictCursor)
    
    selected_region = request.form.get('region_filter', 'All')
    selected_hub = request.form.get('hub_filter', 'All')
    selected_country = request.form.get('country_filter', 'All')
    selected_site = request.form.get('site_filter', 'All')
    
    conditions = []
    params = []
    
    if selected_region and selected_region != 'All':
        conditions.append("region = %s")
        params.append(selected_region)
    if selected_hub and selected_hub != 'All':
        conditions.append("hub = %s")
        params.append(selected_hub)
    if selected_country and selected_country != 'All':
        conditions.append("country = %s")
        params.append(selected_country)
    if selected_site and selected_site != 'All':
        conditions.append("site = %s")
        params.append(selected_site)
        
    where_clause = " AND ".join(conditions)
    
    query = "SELECT region, hub, country, site, rse_count, dse_count, itc_count FROM site_data"
    if where_clause:
        query += " WHERE " + where_clause
    query += " ORDER BY region, hub, country, site"
    
    try:
        cursor.execute(query, tuple(params))
        data = cursor.fetchall()
    except psycopg2.Error as err:
        flash(f"Error fetching data for download: {err}", 'danger')
        cursor.close()
        conn.close()
        return redirect(url_for('index'))
        
    cursor.close()
    conn.close()

    if not data:
        flash("No data found matching the current filters for download.", 'warning')
        return redirect(url_for('index'))

    output = io.StringIO()
    writer = csv.writer(output)

    headers = ['Region', 'Hub', 'Country', 'Site', 'RSE Count', 'DSE Count', 'ITC Count', 'Total Associates']
    writer.writerow(headers)

    for row in data:
        total = row['rse_count'] + row['dse_count'] + row['itc_count']
        writer.writerow([
            row['region'], 
            row['hub'], 
            row['country'], 
            row['site'], 
            row['rse_count'],
            row['dse_count'],
            row['itc_count'],
            total
        ])

    csv_output = output.getvalue()
    filename = 'associate_data_filtered.csv'
    
    response = Response(
        csv_output,
        mimetype='text/csv',
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )
    return response


# --- Main Startup Block ---
if __name__ == '__main__':
    # Initialize the database (runs locally)
    init_db()
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)