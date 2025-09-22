# Yandex 360 Department and User Sync

This script helps you synchronize departments and users with Yandex 360 using their API.

## Setup

1. **Install dependencies:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure API credentials:**
   - Copy `env_template.txt` to `.env`
   - Fill in your actual values:
     ```
     ORG_ID=your_organization_id_here
     API_TOKEN=your_oauth_or_passport_token_here
     ```

## Getting API Credentials

### Organization ID (ORG_ID)
- Log into your Yandex 360 admin panel
- Go to Settings → Organization settings
- Find your Organization ID

### API Token (API_TOKEN)
You can use either:
- **OAuth token**: Follow Yandex OAuth flow
- **Passport token**: Get from Yandex Passport settings

## CSV File Format

### departments.csv
Required columns:
- `external_id`: Unique identifier for the department
- `name`: Display name of the department
- `parent_external_id`: (Optional) External ID of parent department
- `label`: (Optional) Department label
- `description`: (Optional) Department description

### users.csv
Required columns:
- `nickname`: User login/nickname
- `first`: First name
- `last`: Last name
- `dept_external_id`: External ID of the department (must match departments.csv)
- `middle`: (Optional) Middle name
- `position`: (Optional) Job position
- `language`: (Optional) Language (default: "ru")
- `timezone`: (Optional) Timezone (default: "Europe/Moscow")
- `password`: (Optional) User password
- `passwordChangeRequired`: (Optional) "true" or "false" (default: "true")
- `externalId`: (Optional) External user ID

## Usage

### Dry Run (Recommended first)
Test your configuration and CSV data without making actual API calls:

```bash
source venv/bin/activate
python sync360.py --dry-run
```

### Actual Sync
Run the actual synchronization:

```bash
source venv/bin/activate
python sync360.py
```

## Features

- ✅ **Dry run mode**: Test configuration without making API calls
- ✅ **Idempotent**: Safe to run multiple times
- ✅ **Hierarchy support**: Automatically handles department parent-child relationships
- ✅ **Validation**: Validates CSV data before processing
- ✅ **Error handling**: Comprehensive error reporting
- ✅ **Duplicate detection**: Prevents duplicate departments and users
- ✅ **Progress tracking**: Shows detailed progress information

## Troubleshooting

### Common Issues

1. **"ORG_ID environment variable is required"**
   - Make sure you created a `.env` file with your credentials

2. **"404 Client Error: Not found"**
   - Check your ORG_ID is correct
   - Verify your API_TOKEN has proper permissions

3. **"Department not found"**
   - Ensure `dept_external_id` in users.csv matches `external_id` in departments.csv

4. **"Duplicate external_id values found"**
   - Check for duplicate entries in your departments.csv

### API Rate Limits
The script includes automatic retry logic with exponential backoff for rate limits.
