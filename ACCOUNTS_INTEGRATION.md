# ACCOUNTS Dataset Integration

This documentation explains how to integrate the ACCOUNTS dataset into the Customer Lifetime Value application and update the dashboard to display accurate account status statistics.

## Overview

The ACCOUNTS dataset provides detailed information about all customer accounts, including their status:

- **Inoperative Accounts (`ACNTS_INOP_ACNT`)**: Accounts become inoperative after 90 days of inactivity.
- **Dormant Accounts (`ACNTS_DORMANT_ACNT`)**: Accounts become dormant after 90 days of being inoperative.
- **Closed Accounts**: Accounts are considered closed after being dormant for 1 year.

## Prerequisites

1. The ACCOUNTS dataset file (CSV format)
2. Python 3.6 or higher
3. Required Python packages (see `data_processor/requirements.txt`)

## Integration Scripts

The following scripts have been created to facilitate the integration:

1. **import_accounts.py**: Imports the ACCOUNTS dataset into the database, replacing the existing clients table.
2. **update_dashboard_stats.py**: Updates the dashboard statistics based on the imported data.
3. **run_app_with_accounts.py**: A convenience script that imports the data, updates the statistics, and starts the application.

## Steps to Run the Application

### Option 1: Using the Convenience Script

The easiest way to run the application with the ACCOUNTS dataset is to use the provided convenience script:

```
python run_app_with_accounts.py path/to/ACCOUNTS.csv
```

This script will:
1. Import the ACCOUNTS dataset
2. Update the dashboard statistics
3. Start the Flask application

### Option 2: Manual Steps

If you prefer to run each step individually:

1. Import the ACCOUNTS dataset:
   ```
   python import_accounts.py path/to/ACCOUNTS.csv
   ```

2. Update the dashboard statistics:
   ```
   python update_dashboard_stats.py
   ```

3. Start the Flask application:
   ```
   cd data_processor
   python app.py
   ```

## Dashboard Updates

The dashboard has been updated to display the following account statistics:

- **Active Accounts**: Accounts that are neither inoperative nor dormant.
- **Inactive Accounts**: Accounts that are inoperative but not dormant.
- **Dormant Accounts**: Accounts that are marked as dormant.
- **Closed Accounts**: Accounts that have been dormant for over a year.

## Additional Information

- The product lifecycle analysis has been updated to consider account status when determining lifecycle stages.
- Dashboard statistics are cached for performance and updated hourly.

## Troubleshooting

If you encounter issues:

1. Check that the ACCOUNTS dataset file exists and is in CSV format.
2. Ensure the database file is accessible and not locked by another process.
3. Check the application logs for error messages.
4. Try running the steps manually to identify where the issue occurs. 