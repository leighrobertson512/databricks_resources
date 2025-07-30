# Data Discovery Hub - Deployment Steps

Follow these exact steps to deploy your Data Discovery Hub app in Databricks.

## Prerequisites

1. **Databricks CLI installed and configured**
   ```bash
   pip install databricks-cli
   databricks configure --token
   ```

2. **Files ready to upload:**
   - ✅ app.py
   - ✅ app.yaml (newly created)
   - ✅ requirements.txt
   - ✅ data_discovery_utils.py
   - ✅ search_utils.py
   - ✅ visualization_utils.py
   - ✅ config.py

## Step 1: Prepare Your Local Environment

1. **Create a local folder for your app:**
   ```bash
   mkdir my-app-source
   cd my-app-source
   ```

2. **Copy all the app files to this folder:**
   - Copy all Python files from the `observability/` folder
   - Make sure `app.yaml` and `requirements.txt` are included

## Step 2: Upload Files to Databricks

1. **Sync files to your Databricks workspace:**
   ```bash
   databricks sync --watch . /Workspace/Users/leigh.robertson@databricks.com/data-discovery-hub
   ```

   This command will:
   - Upload all files from your current directory
   - Sync to the specified workspace path
   - Watch for changes (you can stop with Ctrl+C after initial sync)

## Step 3: Deploy the App

1. **Deploy using Databricks CLI:**
   ```bash
   databricks apps deploy data-discovery-hub --source-code-path /Workspace/Users/leigh.robertson@databricks.com/data-discovery-hub
   ```

## Step 4: Verify Deployment

1. **Check app status:**
   ```bash
   databricks apps list
   ```

2. **View app logs if there are issues:**
   ```bash
   databricks apps logs data-discovery-hub
   ```

## Troubleshooting

### Common Issues and Solutions:

1. **"Missing package or wrong package version"**
   - **Solution:** The `requirements.txt` file should handle this automatically
   - If issues persist, add missing packages to `requirements.txt`

2. **"Permissions issue"**
   - **Solution:** Run this command to grant access:
     ```bash
     databricks workspace chmod 755 /Workspace/Users/leigh.robertson@databricks.com/data-discovery-hub
     ```

3. **"Missing environment variable"**
   - **Solution:** Environment variables are defined in `app.yaml`
   - Check the `env` section is properly configured

4. **"Running the wrong command line at startup"**
   - **Solution:** The `command` section in `app.yaml` defines the startup command
   - Should run: `streamlit run app.py`

### If the deployment fails:

1. **Check the logs:**
   ```bash
   databricks apps logs data-discovery-hub
   ```

2. **Verify files are uploaded:**
   ```bash
   databricks workspace ls /Workspace/Users/leigh.robertson@databricks.com/data-discovery-hub
   ```

3. **Test locally first** (optional):
   ```bash
   # In your local my-app-source folder
   pip install -r requirements.txt
   streamlit run app.py
   ```

## Alternative: Manual File Upload

If the CLI sync doesn't work, you can manually upload files:

1. **Go to your Databricks workspace**
2. **Navigate to:** `/Workspace/Users/leigh.robertson@databricks.com/`
3. **Create folder:** `data-discovery-hub`
4. **Upload each file manually** through the Databricks UI

## Expected Result

Once deployed successfully, you should be able to:

1. **Access your app** through the Databricks Apps interface
2. **See the Data Discovery Hub interface** with:
   - Overview dashboard
   - Search functionality
   - Interactive charts
   - Table exploration

## Quick Verification

After deployment, the app should show:
- 📊 Data overview metrics
- 🔍 Search box for tables/columns
- 📈 Interactive visualizations
- 💡 Table recommendations

If you see a Streamlit interface with these elements, your deployment was successful! 🎉

---

**Need help?** Check the main README.md for additional troubleshooting tips. 