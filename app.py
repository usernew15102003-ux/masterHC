import sys
import os 
import io
import csv
from flask import Flask, render_template_string, request, flash, Response, redirect, url_for
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
        # Connect directly using the DATABASE_URL environment variable
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


# --- HTML Template Content (Embedded) ---
INDEX_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Associate Data Filter & Edit</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background-color: #f4f7f6; font-family: 'Inter', sans-serif; }
        .filter-card {
            box-shadow: 0 6px 15px rgba(0,0,0,0.15);
            border-radius: 12px;
        }
        .result-card {
            border-left: 5px solid #0d6efd;
            border-radius: 8px;
        }
        .count-display {
            font-size: 3rem;
            font-weight: 700;
            color: #198754; 
            padding: 10px;
            border: 2px solid #198754;
            border-radius: 8px;
            display: inline-block;
            min-width: 150px;
        }
        .form-select.rounded-pill {
            border-radius: 50rem !important;
        }
    </style>
</head>
<body>

    <div class="container my-5">
        <h1 class="text-center mb-4 text-primary">OSS AV Master HC</h1>
        <p class="text-center text-muted">Select criteria to aggregate and display associate data.</p>

        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                <div class="alert alert-{{ category }} alert-dismissible fade show" role="alert">
                    {{ message }}
                    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                </div>
                {% endfor %}
            {% endif %}
        {% endwith %}

        <div class="card p-4 filter-card mb-5">
            <h5 class="card-title text-success mb-3">Filter Options</h5>
            <form method="POST" action="/" class="row g-3">
                
                <div class="col-md-3 col-sm-6">
                    <label for="region_filter" class="form-label">Region:</label>
                    <select name="region_filter" id="region_filter" class="form-select rounded-pill">
                        <option value="All">-- All Regions --</option>
                        {% for region in regions %}
                        <option value="{{ region }}" {% if filter_data and filter_data.region == region %} selected {% endif %}>
                            {{ region }}
                        </option>
                        {% endfor %}
                    </select>
                </div>

                <div class="col-md-3 col-sm-6">
                    <label for="hub_filter" class="form-label">Hub:</label>
                    <select name="hub_filter" id="hub_filter" class="form-select rounded-pill">
                        <option value="All">-- All Hubs --</option>
                        {% for hub in hubs %}
                        <option value="{{ hub }}" {% if filter_data and filter_data.hub == hub %} selected {% endif %}>
                            {{ hub }}
                        </option>
                        {% endfor %}
                    </select>
                </div>
                
                <div class="col-md-3 col-sm-6">
                    <label for="country_filter" class="form-label">Country:</label>
                    <select name="country_filter" id="country_filter" class="form-select rounded-pill">
                        <option value="All">-- All Countries --</option>
                        {% for country in countries %}
                        <option value="{{ country }}" {% if filter_data and filter_data.country == country %} selected {% endif %}>
                            {{ country }}
                        </option>
                        {% endfor %}
                    </select>
                </div>

                <div class="col-md-3 col-sm-6">
                    <label for="site_filter" class="form-label">Site:</label>
                    <select name="site_filter" id="site_filter" class="form-select rounded-pill">
                        <option value="All">-- All Sites --</option>
                        {% for site in sites %}
                        <option value="{{ site }}" {% if filter_data and filter_data.site == site %} selected {% endif %}>
                            {{ site }}
                        </option>
                        {% endfor %}
                    </select>
                </div>
                
                <div class="col-12 mt-4 text-center">
                    <button type="submit" class="btn btn-primary btn-lg px-5 shadow-sm">
                        <i class="fas fa-search me-2"></i> Filter Data
                    </button>
                </div>
            </form>
        </div>

        {% if filter_data %}
        <div class="card p-4 mt-4 result-card bg-white">
            <h4 class="card-title text-primary">🔍 Filtered Result</h4>
            
            <div class="d-flex justify-content-between align-items-center mb-3">
                <p class="card-subtitle mb-0 text-muted">Showing {{ filter_data.filtered_rows|length }} record(s) matching the criteria.</p>
                <form method="POST" action="{{ url_for('download_data') }}" class="m-0">
                    <input type="hidden" name="region_filter" value="{{ filter_data.region }}">
                    <input type="hidden" name="hub_filter" value="{{ filter_data.hub }}">
                    <input type="hidden" name="country_filter" value="{{ filter_data.country }}">
                    <input type="hidden" name="site_filter" value="{{ filter_data.site }}">
                    <button type="submit" class="btn btn-success btn-sm shadow-sm" {% if not filter_data.filtered_rows %} disabled {% endif %}>
                        <i class="fas fa-file-excel me-2"></i> Download Data (CSV)
                    </button>
                </form>
            </div>
            
            <div class="row mb-4 border-bottom pb-3 align-items-center">
                <div class="col-md-6 mb-3 mb-md-0">
                    <p class="mb-1 text-success fw-bold">TOTAL ASSOCIATES (RSE + DSE + ITC):</p>
                    <div class="count-display">{{ filter_data.total_count }}</div>
                </div>
                <div class="col-md-6">
                    <p class="mb-1"><strong>Selected Filters:</strong></p>
                    <span class="badge bg-primary me-2">Region: {{ filter_data.region }}</span>
                    <span class="badge bg-primary me-2">Hub: {{ filter_data.hub }}</span>
                    <span class="badge bg-primary me-2">Country: {{ filter_data.country }}</span>
                    <span class="badge bg-primary">Site: {{ filter_data.site }}</span>
                </div>
            </div>

            {% if filter_data.filtered_rows %}
            <div class="table-responsive mt-3">
                <table class="table table-striped table-hover rounded">
                    <thead class="table-dark">
                        <tr>
                            <th>Region</th>
                            <th>Hub</th>
                            <th>Country</th>
                            <th>Site</th>
                            <th class="text-end">RSE Count</th>
                            <th class="text-end">DSE Count</th>
                            <th class="text-end">ITC Count</th>
                            <th class="text-end">Total Associates</th>
                            <th>Action</th> </tr>
                    </thead>
                    <tbody>
                        {% for row in filter_data.filtered_rows %}
                        <tr>
                            <td data-region="{{ row.region }}">{{ row.region }}</td>
                            <td data-hub="{{ row.hub }}">{{ row.hub }}</td>
                            <td data-country="{{ row.country }}">{{ row.country }}</td>
                            <td data-site="{{ row.site }}">{{ row.site }}</td>
                            <td class="text-end" data-rse="{{ row.rse_count }}">{{ row.rse_count }}</td>
                            <td class="text-end" data-dse="{{ row.dse_count }}">{{ row.dse_count }}</td>
                            <td class="text-end" data-itc="{{ row.itc_count }}">{{ row.itc_count }}</td>
                            <td class="text-end fw-bold">{{ row.rse_count + row.dse_count + row.itc_count }}</td>
                            <td>
                                <button type="button" class="btn btn-warning btn-sm edit-btn" 
                                    data-bs-toggle="modal" 
                                    data-bs-target="#editModal"
                                    data-id="{{ row.id }}"
                                    data-region="{{ row.region }}"
                                    data-hub="{{ row.hub }}"
                                    data-country="{{ row.country }}"
                                    data-site="{{ row.site }}"
                                    data-rse="{{ row.rse_count }}"
                                    data-dse="{{ row.dse_count }}"
                                    data-itc="{{ row.itc_count }}">
                                    <i class="fas fa-edit"></i> Edit
                                </button>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            {% else %}
                <div class="alert alert-warning mt-3" role="alert">
                    No records found matching the selected filters.
                </div>
            {% endif %}

        </div>
        {% endif %}
    </div>

    <div class="modal fade" id="editModal" tabindex="-1" aria-labelledby="editModalLabel" aria-hidden="true">
        <div class="modal-dialog">
            <div class="modal-content">
                <form method="POST" action="/edit_data/0" id="editForm">
                    <div class="modal-header bg-warning text-white">
                        <h5 class="modal-title" id="editModalLabel">Edit Associate Counts</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <p class="text-muted">Editing data for: <strong id="modal_location"></strong></p>
                        <input type="hidden" name="row_id" id="modal_row_id">

                        <div class="mb-3">
                            <label for="rse_count" class="form-label">RSE Count</label>
                            <input type="number" class="form-control" id="rse_count" name="rse_count" required min="0">
                        </div>
                        <div class="mb-3">
                            <label for="dse_count" class="form-label">DSE Count</label>
                            <input type="number" class="form-control" id="dse_count" name="dse_count" required min="0">
                        </div>
                        <div class="mb-3">
                            <label for="itc_count" class="form-label">ITC Count</label>
                            <input type="number" class="form-control" id="itc_count" name="itc_count" required min="0">
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                        <button type="submit" class="btn btn-warning">Save Changes</button>
                    </div>
                </form>
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/js/all.min.js"></script>
    
    <script>
    document.addEventListener('DOMContentLoaded', function () {
        const editModal = document.getElementById('editModal');
        editModal.addEventListener('show.bs.modal', function (event) {
            const button = event.relatedTarget; 
            const id = button.getAttribute('data-id');
            const region = button.getAttribute('data-region');
            const hub = button.getAttribute('data-hub');
            const country = button.getAttribute('data-country');
            const site = button.getAttribute('data-site');
            const rse = button.getAttribute('data-rse');
            const dse = button.getAttribute('data-dse');
            const itc = button.getAttribute('data-itc');
            document.getElementById('modal_location').textContent = `${region} - ${hub} - ${country} - ${site}`;
            document.getElementById('modal_row_id').value = id;
            document.getElementById('rse_count').value = rse;
            document.getElementById('dse_count').value = dse;
            document.getElementById('itc_count').value = itc;
            document.getElementById('editForm').action = `/edit_data/${id}`;
        });
    });
    </script>
</body>
</html>
"""

# --- Routes ---

@app.route('/', methods=['GET', 'POST'])
def index():
    conn = get_db_connection()
    if conn is None:
        flash('Database connection failed. Check server status and configuration.', 'danger')
        return render_template_string(INDEX_HTML_TEMPLATE, regions=[], hubs=[], countries=[], sites=[], filter_data=None, category='danger')
        
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

    return render_template_string(
        INDEX_HTML_TEMPLATE,
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