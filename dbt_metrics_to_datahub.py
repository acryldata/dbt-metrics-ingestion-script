#!/usr/bin/env python3
"""
dbt Metrics to DataHub Ingestion Script

This script reads a dbt manifest.json file and ingests metrics as GlossaryTerms
into DataHub, creating lineage to the underlying models/sources.

Usage:
    python dbt_metrics_to_datahub.py --manifest /path/to/manifest.json \
                                      --datahub-url http://localhost:8080 \
                                      --token <your-token>

Requirements:
    pip install acryl-datahub
"""

import argparse
import json
import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

from datahub.emitter.mce_builder import (
    make_term_urn,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlossaryTermInfoClass,
    GlossaryNodeInfoClass,
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def make_glossary_node_urn(node_name: str) -> str:
    """Create a GlossaryNode URN from a node name"""
    return f"urn:li:glossaryNode:{node_name}"


@dataclass
class DBTMetric:
    """Represents a dbt metric from manifest.json"""
    name: str
    unique_id: str
    description: Optional[str]
    label: Optional[str]
    type: Optional[str]  # simple, ratio, derived, cumulative
    calculation_method: Optional[str]
    expression: Optional[str]
    filters: List[Dict]
    dimensions: List[str]
    time_grains: List[str]
    depends_on: List[str]  # models, sources this metric depends on
    meta: Dict
    tags: List[str]
    package_name: str
    path: str


@dataclass
class DBTSemanticModel:
    """Represents a dbt semantic model (dbt 1.6+)"""
    name: str
    unique_id: str
    description: Optional[str]
    model: str  # ref to underlying model
    dimensions: List[Dict]
    measures: List[Dict]
    entities: List[Dict]
    meta: Dict


class DBTMetricsIngestion:
    """Handles ingestion of dbt metrics into DataHub"""

    def __init__(
        self,
        datahub_url: str,
        token: Optional[str] = None,
        platform: str = "dbt",
        env: str = "PROD",
        glossary_root: str = "dbt_metrics",
        dry_run: bool = False
    ):
        self.dry_run = dry_run
        if not dry_run:
            self.emitter = DatahubRestEmitter(
                gms_server=datahub_url,
                token=token
            )
        else:
            self.emitter = None
            logger.info("DRY RUN MODE - No data will be emitted to DataHub")
        self.platform = platform
        self.env = env
        self.glossary_root = glossary_root

    def emit(self, mcpw):
        """Emit metadata change proposal (or skip in dry-run mode)"""
        if self.dry_run:
            logger.debug(f"[DRY RUN] Would emit: {mcpw.entityUrn}")
        else:
            self.emitter.emit_mcp(mcpw)

    def load_manifest(self, manifest_path: str) -> Dict:
        """Load dbt manifest.json"""
        logger.info(f"Loading manifest from {manifest_path}")
        with open(manifest_path, 'r') as f:
            return json.load(f)

    def parse_metrics(self, manifest: Dict) -> List[DBTMetric]:
        """Extract metrics from manifest"""
        metrics = []

        for metric_id, metric_data in manifest.get('metrics', {}).items():
            metric = DBTMetric(
                name=metric_data.get('name'),
                unique_id=metric_id,
                description=metric_data.get('description', ''),
                label=metric_data.get('label'),
                type=metric_data.get('type'),
                calculation_method=metric_data.get('calculation_method'),
                expression=metric_data.get('expression'),
                filters=metric_data.get('filters', []),
                dimensions=metric_data.get('dimensions', []),
                time_grains=metric_data.get('time_grains', []),
                depends_on=metric_data.get('depends_on', {}).get('nodes', []),
                meta=metric_data.get('meta', {}),
                tags=metric_data.get('tags', []),
                package_name=metric_data.get('package_name', ''),
                path=metric_data.get('path', '')
            )
            metrics.append(metric)

        logger.info(f"Found {len(metrics)} metrics in manifest")
        return metrics

    def parse_semantic_models(self, manifest: Dict) -> List[DBTSemanticModel]:
        """Extract semantic models from manifest (dbt 1.6+)"""
        semantic_models = []

        for sm_id, sm_data in manifest.get('semantic_models', {}).items():
            sm = DBTSemanticModel(
                name=sm_data.get('name'),
                unique_id=sm_id,
                description=sm_data.get('description', ''),
                model=sm_data.get('model'),
                dimensions=sm_data.get('dimensions', []),
                measures=sm_data.get('measures', []),
                entities=sm_data.get('entities', []),
                meta=sm_data.get('meta', {})
            )
            semantic_models.append(sm)

        logger.info(f"Found {len(semantic_models)} semantic models in manifest")
        return semantic_models

    def create_glossary_hierarchy(self, metrics: List[DBTMetric]) -> Dict[str, str]:
        """Create glossary nodes for organizing metrics. Returns dict of category -> node URN"""
        categories = {}

        for metric in metrics:
            # Extract category from meta tags or use default
            category = metric.meta.get('datahub_glossary_category', 'Uncategorized')
            categories[category] = None  # Will be filled with URN

        # Create root glossary node
        root_urn = make_glossary_node_urn(self.glossary_root)
        root_info = GlossaryNodeInfoClass(
            definition="dbt metrics ingested from dbt project",
            name=self.glossary_root
        )

        mcpw = MetadataChangeProposalWrapper(
            entityUrn=root_urn,
            aspect=root_info
        )
        self.emit(mcpw)
        logger.info(f"Created glossary root node: {self.glossary_root}")

        # Create category nodes
        for category in categories.keys():
            # Handle nested categories (e.g., "Finance/Revenue")
            category_path = category.replace('/', '.')
            category_urn = make_glossary_node_urn(f"{self.glossary_root}.{category_path}")
            category_info = GlossaryNodeInfoClass(
                definition=f"Metrics in category: {category}",
                name=category.split('/')[-1],  # Use last part as display name
                parentNode=root_urn
            )

            mcpw = MetadataChangeProposalWrapper(
                entityUrn=category_urn,
                aspect=category_info
            )
            self.emit(mcpw)
            logger.info(f"Created glossary category node: {category}")

            # Store URN for later reference
            categories[category] = category_urn

        return categories

    def resolve_node_to_dataset_urn(
        self,
        node_id: str,
        manifest: Dict
    ) -> Optional[str]:
        """Resolve a dbt node ID to a DataHub dataset URN"""
        # Check if it's a model
        if node_id in manifest.get('nodes', {}):
            node = manifest['nodes'][node_id]
            database = node.get('database', '')
            schema = node.get('schema', '')
            identifier = node.get('alias') or node.get('name')

            # Build dataset name: database.schema.table
            dataset_name = f"{database}.{schema}.{identifier}".lower()
            return make_dataset_urn(
                platform=self.platform,
                name=dataset_name,
                env=self.env
            )

        # Check if it's a source
        if node_id in manifest.get('sources', {}):
            source = manifest['sources'][node_id]
            source_database = source.get('database', '')
            source_schema = source.get('schema', '')
            source_identifier = source.get('identifier') or source.get('name')

            dataset_name = f"{source_database}.{source_schema}.{source_identifier}".lower()
            return make_dataset_urn(
                platform=self.platform,
                name=dataset_name,
                env=self.env
            )

        logger.warning(f"Could not resolve node {node_id} to dataset URN")
        return None

    def emit_metric_as_glossary_term(
        self,
        metric: DBTMetric,
        manifest: Dict,
        category_urns: Dict[str, str]
    ) -> str:
        """Emit a single metric as a GlossaryTerm"""
        # Determine category
        category = metric.meta.get('datahub_glossary_category', 'Uncategorized')
        category_path = category.replace('/', '.')
        term_name = f"{self.glossary_root}.{category_path}.{metric.name}"
        term_urn = make_term_urn(term_name)

        # Build custom properties
        custom_props = {
            'dbt_unique_id': metric.unique_id,
            'dbt_package': metric.package_name,
            'dbt_path': metric.path,
        }

        if metric.type:
            custom_props['metric_type'] = metric.type
        if metric.calculation_method:
            custom_props['calculation_method'] = metric.calculation_method
        if metric.expression:
            custom_props['expression'] = metric.expression
        if metric.dimensions:
            custom_props['dimensions'] = ', '.join(metric.dimensions)
        if metric.time_grains:
            custom_props['time_grains'] = ', '.join(metric.time_grains)
        if metric.filters:
            custom_props['filters'] = json.dumps(metric.filters)
        if metric.tags:
            custom_props['tags'] = ', '.join(metric.tags)

        # Add upstream dataset lineage (as custom property since GlossaryTerms don't support upstreamLineage)
        if metric.depends_on:
            upstream_urns = []
            for dep in metric.depends_on:
                dataset_urn = self.resolve_node_to_dataset_urn(dep, manifest)
                if dataset_urn:
                    upstream_urns.append(dataset_urn)
            if upstream_urns:
                custom_props['upstream_datasets'] = ', '.join(upstream_urns)

        # Add any custom meta properties
        for key, value in metric.meta.items():
            if key not in ['datahub_glossary_category']:
                custom_props[f'meta_{key}'] = str(value)

        # Get parent node URN for this category
        parent_node_urn = category_urns.get(category)

        # Create GlossaryTermInfo with parentNode link
        term_info = GlossaryTermInfoClass(
            definition=metric.description or f"dbt metric: {metric.name}",
            name=metric.label or metric.name,
            customProperties=custom_props,
            termSource="dbt",
            parentNode=parent_node_urn  # Link term to its category node
        )

        mcpw = MetadataChangeProposalWrapper(
            entityUrn=term_urn,
            aspect=term_info
        )
        self.emit(mcpw)

        # Note: GlossaryTerms don't support globalTags aspect
        # Tags are stored in customProperties instead
        if metric.tags:
            logger.debug(f"Tags for metric '{metric.name}': {metric.tags} (stored in customProperties)")

        logger.info(f"Emitted metric '{metric.name}' as GlossaryTerm: {term_urn}")

        # Note: GlossaryTerms don't support upstreamLineage aspect
        # Lineage info is already stored in customProperties above
        # The upstream datasets are referenced in 'depends_on' field

        return term_urn

    def ingest_metrics(self, manifest_path: str):
        """Main ingestion flow"""
        logger.info("Starting dbt metrics ingestion...")

        # Load manifest
        manifest = self.load_manifest(manifest_path)

        # Parse metrics
        metrics = self.parse_metrics(manifest)

        if not metrics:
            logger.warning("No metrics found in manifest. Exiting.")
            return

        # Create glossary hierarchy (returns dict of category -> URN)
        category_urns = self.create_glossary_hierarchy(metrics)

        # Emit each metric
        for metric in metrics:
            try:
                self.emit_metric_as_glossary_term(metric, manifest, category_urns)
            except Exception as e:
                logger.error(f"Failed to emit metric '{metric.name}': {e}")

        logger.info(f"✅ Successfully ingested {len(metrics)} metrics into DataHub!")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest dbt metrics into DataHub as GlossaryTerms"
    )
    parser.add_argument(
        '--manifest',
        required=True,
        help='Path to dbt manifest.json file'
    )
    parser.add_argument(
        '--datahub-url',
        default='http://localhost:8080',
        help='DataHub GMS URL (default: http://localhost:8080)'
    )
    parser.add_argument(
        '--token',
        help='DataHub authentication token (optional)'
    )
    parser.add_argument(
        '--platform',
        default='dbt',
        help='Platform name for lineage (default: dbt)'
    )
    parser.add_argument(
        '--env',
        default='PROD',
        help='Environment for lineage (default: PROD)'
    )
    parser.add_argument(
        '--glossary-root',
        default='dbt_metrics',
        help='Root glossary node name (default: dbt_metrics)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse and validate without emitting to DataHub'
    )

    args = parser.parse_args()

    # Create ingestion instance
    ingestion = DBTMetricsIngestion(
        datahub_url=args.datahub_url,
        token=args.token,
        platform=args.platform,
        env=args.env,
        glossary_root=args.glossary_root,
        dry_run=args.dry_run
    )

    # Run ingestion
    ingestion.ingest_metrics(args.manifest)


if __name__ == '__main__':
    main()
