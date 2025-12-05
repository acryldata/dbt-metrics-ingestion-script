# Testing Guide for dbt Metrics Ingestion Script

This guide walks you through testing the script before running it in production.

## Option 1: Test with Your Own dbt Project

### Step 1: Generate Fresh Manifest

```bash
cd your-dbt-project/
dbt compile  # or dbt run
```

This creates `target/manifest.json` with your metrics.

### Step 2: Verify Metrics Exist

```bash
# Quick check: how many metrics are in the manifest?
python3 -c "
import json
with open('target/manifest.json') as f:
    manifest = json.load(f)
    metrics = manifest.get('metrics', {})
    print(f'✅ Found {len(metrics)} metrics')
    if metrics:
        print('\nMetrics:')
        for m_id, m in list(metrics.items())[:5]:  # Show first 5
            print(f'  - {m[\"name\"]}: {m.get(\"type\", \"unknown\")} metric')
"
```

### Step 3: Run the Script (Dry Run First)

Add debug logging to see what would be emitted:

```bash
# Set up environment
pip install acryl-datahub

# Run with verbose logging
python dbt_metrics_to_datahub.py \
  --manifest target/manifest.json \
  --datahub-url http://your-datahub:8080 \
  --token your-token \
  2>&1 | tee ingestion.log
```

Check `ingestion.log` for:
- ✅ "Found X metrics in manifest"
- ✅ "Created glossary root: dbt_metrics"
- ✅ "Created glossary category: Finance" (etc.)
- ✅ "Emitted metric 'revenue_total' as GlossaryTerm"
- ✅ "Created lineage for metric 'revenue_total' to N datasets"

### Step 4: Verify in DataHub UI

1. Go to https://your-datahub-instance.com
2. Click **Glossary** in the left nav
3. Look for `dbt_metrics` node
4. Expand and verify:
   - Metrics appear under correct categories
   - Click a metric → See description, custom properties
   - Click **Lineage** tab → See upstream datasets

---

## Option 2: Test with Sample Manifest (No dbt Project Needed)

### Step 1: Create Sample Manifest

Save this as `sample_manifest.json`:

```json
{
  "metadata": {
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
    "project_name": "test_project"
  },
  "metrics": {
    "metric.test_project.revenue_total": {
      "name": "revenue_total",
      "label": "Total Revenue",
      "description": "Sum of all revenue across all channels",
      "type": "simple",
      "calculation_method": "sum",
      "expression": "revenue_amount",
      "dimensions": ["date", "region"],
      "time_grains": ["day", "week", "month"],
      "filters": [],
      "depends_on": {
        "nodes": ["model.test_project.revenue_by_date"]
      },
      "meta": {
        "datahub_glossary_category": "Finance"
      },
      "tags": ["finance", "kpi"],
      "package_name": "test_project",
      "path": "metrics/revenue.yml"
    },
    "metric.test_project.customer_count": {
      "name": "customer_count",
      "label": "Active Customer Count",
      "description": "Count of distinct active customers",
      "type": "simple",
      "calculation_method": "count_distinct",
      "expression": "customer_id",
      "dimensions": ["segment"],
      "time_grains": ["day", "month"],
      "filters": [
        {
          "field": "is_active",
          "operator": "=",
          "value": true
        }
      ],
      "depends_on": {
        "nodes": ["model.test_project.customers"]
      },
      "meta": {
        "datahub_glossary_category": "Marketing"
      },
      "tags": ["marketing", "customers"],
      "package_name": "test_project",
      "path": "metrics/customers.yml"
    }
  },
  "nodes": {
    "model.test_project.revenue_by_date": {
      "name": "revenue_by_date",
      "database": "analytics",
      "schema": "finance",
      "alias": "revenue_by_date",
      "resource_type": "model"
    },
    "model.test_project.customers": {
      "name": "customers",
      "database": "analytics",
      "schema": "marketing",
      "alias": "customers",
      "resource_type": "model"
    }
  },
  "sources": {}
}
```

### Step 2: Run Against Sample

```bash
python dbt_metrics_to_datahub.py \
  --manifest sample_manifest.json \
  --datahub-url http://localhost:8080
```

**Expected Output:**
```
2024-12-05 10:30:00 - INFO - Loading manifest from sample_manifest.json
2024-12-05 10:30:00 - INFO - Found 2 metrics in manifest
2024-12-05 10:30:00 - INFO - Found 0 semantic models in manifest
2024-12-05 10:30:01 - INFO - Created glossary root: dbt_metrics
2024-12-05 10:30:01 - INFO - Created glossary category: Finance
2024-12-05 10:30:01 - INFO - Created glossary category: Marketing
2024-12-05 10:30:02 - INFO - Emitted metric 'revenue_total' as GlossaryTerm: urn:li:glossaryTerm:dbt_metrics.Finance.revenue_total
2024-12-05 10:30:02 - INFO - Created lineage for metric 'revenue_total' to 1 datasets
2024-12-05 10:30:03 - INFO - Emitted metric 'customer_count' as GlossaryTerm: urn:li:glossaryTerm:dbt_metrics.Marketing.customer_count
2024-12-05 10:30:03 - INFO - Created lineage for metric 'customer_count' to 1 datasets
2024-12-05 10:30:03 - INFO - ✅ Successfully ingested 2 metrics into DataHub!
```

### Step 3: Verify Results

In DataHub UI, you should see:

```
Glossary
└── dbt_metrics
    ├── Finance
    │   └── revenue_total
    │       - Description: "Sum of all revenue across all channels"
    │       - Properties: metric_type=simple, dimensions=date, region
    │       - Lineage: → analytics.finance.revenue_by_date
    └── Marketing
        └── customer_count
            - Description: "Count of distinct active customers"
            - Properties: metric_type=simple, filters=is_active=true
            - Lineage: → analytics.marketing.customers
```

---

## Option 3: Test MCP Integration (End-to-End)

Once metrics are in DataHub, test the full MCP flow:

### Step 1: Verify DataHub MCP Server

```bash
# Check if MCP server is running and exposing glossary endpoints
curl http://your-datahub:8080/mcp/capabilities
```

Look for glossary/search or glossary/get endpoints.

### Step 2: Test MCP Retrieval

```bash
# Manually test MCP can retrieve glossary terms
curl -X POST http://your-datahub:8080/mcp/tools/glossary/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "revenue_total"
  }'
```

**Expected:** JSON response with the metric metadata.

### Step 3: Connect Snowflake Intelligence

Follow Snowflake Intelligence documentation to:
1. Configure MCP connection to DataHub
2. Set up authentication
3. Test queries like:
   - "What is the definition of revenue_total?"
   - "How is customer_count calculated?"
   - "Show me all metrics in the Finance category"

Snowflake Intelligence should retrieve metadata from DataHub via MCP! 🎉

---

## Troubleshooting Common Issues

### Issue: Script runs but no metrics appear in UI

**Debug Steps:**

1. **Check logs for errors:**
   ```bash
   grep -i error ingestion.log
   ```

2. **Verify DataHub connection:**
   ```bash
   curl http://your-datahub:8080/health
   ```

3. **Check authentication:**
   ```bash
   # Test with a simple entity fetch
   curl -H "Authorization: Bearer your-token" \
        http://your-datahub:8080/entities
   ```

4. **Search for emitted URNs directly:**
   ```bash
   # In DataHub UI search bar, paste the URN from logs:
   urn:li:glossaryTerm:dbt_metrics.Finance.revenue_total
   ```

### Issue: Lineage not showing

**Possible Causes:**
- The underlying datasets don't exist in DataHub yet
- Platform name mismatch (script uses `dbt`, but data is ingested as `snowflake`)

**Solution:**
1. Run your regular dbt ingestion first (to create Dataset entities)
2. Then run metrics ingestion
3. Or: Change `--platform` flag to match your data platform:
   ```bash
   python dbt_metrics_to_datahub.py \
     --manifest target/manifest.json \
     --platform snowflake  # Match your actual platform!
   ```

### Issue: Categories not appearing correctly

**Debug:**

```python
# Check what categories exist in your metrics
import json
with open('target/manifest.json') as f:
    manifest = json.load(f)
    for m_id, m in manifest['metrics'].items():
        category = m.get('meta', {}).get('datahub_glossary_category', 'Uncategorized')
        print(f"{m['name']}: {category}")
```

Make sure your dbt metrics have the `meta.datahub_glossary_category` field set!

---

## Success Criteria Checklist

Before declaring the POC successful, verify:

- [ ] Script runs without errors
- [ ] All expected metrics appear in DataHub Glossary
- [ ] Metrics are organized into correct categories
- [ ] Each metric has:
  - [ ] Correct name and description
  - [ ] Custom properties (metric_type, dimensions, filters, etc.)
  - [ ] Tags preserved from dbt
  - [ ] Lineage to at least one upstream dataset
- [ ] DataHub MCP exposes glossary search/retrieval
- [ ] Snowflake Intelligence can retrieve metric metadata via MCP
- [ ] Users can ask questions like "What is the definition of X?" and get answers

---

## Performance Benchmarks

For reference, typical ingestion times:

| # of Metrics | Ingestion Time |
|--------------|----------------|
| 10 metrics   | ~2-3 seconds   |
| 50 metrics   | ~10-15 seconds |
| 100 metrics  | ~20-30 seconds |
| 500 metrics  | ~2-3 minutes   |

If you have hundreds of metrics, consider:
- Running in batches
- Parallelizing with multiple script instances
- Using asynchronous emission (requires code changes)

---

## Next: Production Deployment

Once testing is successful, see **README.md** for:
- CI/CD integration
- Automated scheduling
- Monitoring and alerting
- Long-term maintenance plan

---

**Questions?** Reach out to Bart in Slack! 🏄‍♂️
