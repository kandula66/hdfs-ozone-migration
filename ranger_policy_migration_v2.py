#!/usr/bin/env python3
"""
Ranger Policy Converter for HDFS to Ozone Migration
This script:
1. Exports Hive policies and creates Ozone clones with database prefix
2. Converts HDFS path-based policies to Ozone volume/bucket/key policies
3. Filters and imports policies back to Ranger
"""

import os
import sys
import requests
import json
import logging
import re
import argparse
import subprocess
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from collections import OrderedDict
import urllib3

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class RangerPolicyConverter:
    def __init__(self, config_file: str):
        self.config = self.load_config(config_file)
        self.setup_logging()
        
        # Hive policies
        self.original_hive_policies = []
        self.filtered_hive_policies = []
        self.cloned_hive_policies = []
        
        # HDFS policies
        self.original_hdfs_policies = []
        self.converted_ozone_policies = []
        
    def load_config(self, config_file: str) -> Dict[str, str]:
        """Load configuration from INI file"""
        config = {}
        try:
            with open(config_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip().strip('"')
            return config
        except Exception as e:
            print(f"ERROR: Failed to load config file: {e}")
            sys.exit(1)
    
    def load_list_from_file(self, filepath: str) -> List[str]:
        """Load a list of items from a file (comma-separated, ignoring comments and empty lines)"""
        items = []
        try:
            with open(filepath, 'r') as f:
                content = f.read()
                
                # Remove comments (lines starting with #)
                lines = []
                for line in content.split('\n'):
                    line = line.strip()
                    # Skip empty lines and comments
                    if line and not line.startswith('#'):
                        lines.append(line)
                
                # Join all lines and split by comma
                combined = ' '.join(lines)
                items = [item.strip() for item in combined.split(',') if item.strip()]
                
            self.logger.info(f"Loaded {len(items)} items from file: {filepath}")
            return items
        except FileNotFoundError:
            self.logger.error(f"File not found: {filepath}")
            return []
        except Exception as e:
            self.logger.error(f"Error reading file {filepath}: {e}")
            return []
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = f"/tmp/ranger_policy_converter/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"policy_converter_{datetime.now().strftime('%H%M%S')}.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Log file: {log_file}")
    
    # ==================== COMMON METHODS ====================
    
    def export_policies(self, service_name: str, service_type: str) -> List[Dict]:
        """Export policies from Ranger for given service"""
        url = f"{self.config['RANGER_URL']}:{self.config['PORT']}/service/plugins/policies/exportJson"
        params = {
            'serviceName': service_name,
            'checkPoliciesExists': 'true'
        }
        
        # Configurable timeout - default 300 seconds (5 minutes) for large exports
        timeout = int(self.config.get('EXPORT_TIMEOUT', '300'))
        
        self.logger.info(f"Exporting {service_type} policies from: {url}")
        self.logger.info(f"Service name: {service_name}")
        self.logger.info(f"Timeout: {timeout} seconds")
        
        try:
            response = requests.get(
                url,
                params=params,
                auth=(self.config['USERNAME'], self.config['PASSWORD']),
                verify=False,
                timeout=timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                policies = data.get('policies', [])
                self.logger.info(f"Successfully exported {len(policies)} {service_type} policies")
                return policies
            else:
                self.logger.error(f"Failed to export policies. HTTP Status: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error exporting policies: {e}")
            return []
    
    def import_policy(self, policy: Dict) -> bool:
        """Import a single policy to Ranger"""
        url = f"{self.config['RANGER_URL']}:{self.config['PORT']}/service/plugins/policies"
        
        # Configurable timeout - default 60 seconds for individual policy import
        timeout = int(self.config.get('IMPORT_TIMEOUT', '60'))
        
        self.logger.info(f"Importing policy: {policy.get('name')}")
        
        try:
            response = requests.post(
                url,
                auth=(self.config['USERNAME'], self.config['PASSWORD']),
                headers={'Content-Type': 'application/json'},
                json=policy,
                verify=False,
                timeout=timeout
            )
            
            if response.status_code == 200:
                self.logger.info(f"Successfully imported policy: {policy.get('name')}")
                return True
            else:
                self.logger.error(f"Failed to import policy {policy.get('name')}. HTTP Status: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error importing policy {policy.get('name')}: {e}")
            return False
    
    def import_policies(self, policies: List[Dict]) -> Dict[str, int]:
        """Import multiple policies to Ranger with progress tracking"""
        results = {'success': 0, 'failed': 0}
        total = len(policies)
        
        self.logger.info(f"Starting import of {total} policies")
        
        for i, policy in enumerate(policies, 1):
            if self.import_policy(policy):
                results['success'] += 1
            else:
                results['failed'] += 1
            
            # Progress updates every 10 policies or at end
            if i % 10 == 0 or i == total:
                progress_pct = (i * 100) // total
                self.logger.info(f"Progress: {i}/{total} ({progress_pct}%) - "
                               f"Success: {results['success']}, Failed: {results['failed']}")
        
        self.logger.info(f"Import completed. Success: {results['success']}, Failed: {results['failed']}")
        return results
    
    def save_policies_to_file(self, policies: List[Dict], filename: str):
        """Save policies to a JSON file"""
        output_dir = f"/tmp/ranger_policy_converter/{datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(output_dir, exist_ok=True)
        
        filepath = os.path.join(output_dir, filename)
        
        try:
            with open(filepath, 'w') as f:
                json.dump({'policies': policies}, f, indent=2)
            self.logger.info(f"Saved {len(policies)} policies to: {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"Failed to save policies to file: {e}")
            return None
    # ==================== HIVE POLICY CLEANUP METHODS ====================
    
    def delete_policy(self, policy_id: int) -> bool:
        """Delete a single policy from Ranger by ID"""
        url = f"{self.config['RANGER_URL']}:{self.config['PORT']}/service/plugins/policies/{policy_id}"
        
        timeout = int(self.config.get('DELETE_TIMEOUT', '60'))
        
        self.logger.info(f"Deleting policy ID: {policy_id}")
        
        try:
            response = requests.delete(
                url,
                auth=(self.config['USERNAME'], self.config['PASSWORD']),
                verify=False,
                timeout=timeout
            )
            
            if response.status_code == 204:
                self.logger.info(f"Successfully deleted policy ID: {policy_id}")
                return True
            else:
                self.logger.error(f"Failed to delete policy ID {policy_id}. HTTP Status: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error deleting policy ID {policy_id}: {e}")
            return False
    
    def has_url_resource(self, policy: Dict) -> bool:
        """Check if policy has URL resource defined"""
        resources = policy.get('resources', {})
        url_resource = resources.get('url', {})
        url_values = url_resource.get('values', [])
        return bool(url_values)
    
    def get_policies_for_cleanup(self, policies: List[Dict], include_databases: List[str] = None) -> Tuple[List[Dict], List[Dict]]:
        """
        Get policies that should be deleted and those that should be kept.
        
        Returns:
            Tuple of (policies_to_delete, policies_to_keep)
        """
        policies_to_delete = []
        policies_to_keep = []
        
        for policy in policies:
            # Skip default policies
            if self.is_default_policy(policy):
                self.logger.debug(f"Keeping default policy: {policy.get('name')}")
                policies_to_keep.append(policy)
                continue
            
            # Skip URL policies
            if self.has_url_resource(policy):
                self.logger.debug(f"Keeping URL policy: {policy.get('name')}")
                policies_to_keep.append(policy)
                continue
            
            # Check database filter
            if include_databases:
                if self.matches_database_filter(policy, include_databases, None):
                    policies_to_delete.append(policy)
                else:
                    policies_to_keep.append(policy)
            else:
                # No filter specified - delete all non-default, non-URL policies
                policies_to_delete.append(policy)
        
        self.logger.info(f"Cleanup analysis:")
        self.logger.info(f"  Total policies: {len(policies)}")
        self.logger.info(f"  Policies to delete: {len(policies_to_delete)}")
        self.logger.info(f"  Policies to keep: {len(policies_to_keep)}")
        
        return policies_to_delete, policies_to_keep
    
    def cleanup_hive_policies(self, policies_to_delete: List[Dict]) -> Dict[str, int]:
        """Delete multiple policies from Ranger with progress tracking"""
        results = {'success': 0, 'failed': 0}
        total = len(policies_to_delete)
        
        if total == 0:
            self.logger.info("No policies to delete")
            return results
        
        self.logger.info(f"Starting cleanup of {total} policies")
        
        for i, policy in enumerate(policies_to_delete, 1):
            policy_id = policy.get('id')
            policy_name = policy.get('name')
            
            if not policy_id:
                self.logger.error(f"Policy {policy_name} has no ID, skipping")
                results['failed'] += 1
                continue
            
            if self.delete_policy(policy_id):
                results['success'] += 1
            else:
                results['failed'] += 1
            
            # Progress updates every 10 policies or at end
            if i % 10 == 0 or i == total:
                progress_pct = (i * 100) // total
                self.logger.info(f"Progress: {i}/{total} ({progress_pct}%) - "
                               f"Success: {results['success']}, Failed: {results['failed']}")
        
        self.logger.info(f"Cleanup completed. Success: {results['success']}, Failed: {results['failed']}")
        return results
    
    # ==================== HIVE POLICY METHODS ==================== 
    # ==================== HIVE POLICY METHODS ====================
    
    def is_default_policy(self, policy: Dict) -> bool:
        """Check if policy is a default policy that should be filtered out"""
        policy_name = policy.get('name', '').lower()
        
        if 'all' in policy_name:
            return True
        
        resources = policy.get('resources', {})
        database = resources.get('database', {})
        db_values = database.get('values', [])
        
        if 'default' in [db.lower() for db in db_values]:
            return True
        
        if 'information_schema' in [db.lower() for db in db_values]:
            return True
        
        return False
    
    def matches_database_filter(self, policy: Dict, include_databases: List[str] = None, 
                                exclude_databases: List[str] = None) -> bool:
        """Check if policy matches database filter criteria"""
        resources = policy.get('resources', {})
        
        database = resources.get('database', {})
        db_values = [db.lower() for db in database.get('values', [])]
        
        url_resource = resources.get('url', {})
        url_values = url_resource.get('values', [])
        
        # Extract database names from URLs (both HDFS and OFS formats)
        for url in url_values:
            # HDFS pattern: hdfs://ns1/data/fid2/raw/hive/hdfs_db4
            hdfs_pattern = r'hdfs://[^/]+/data/([^/]+)/(raw|managed|work)/hive/([^/\s]+)'
            hdfs_match = re.search(hdfs_pattern, url)
            
            if hdfs_match:
                url_db_name = hdfs_match.group(3).lower()
                if url_db_name not in db_values:
                    db_values.append(url_db_name)
            else:
                # OFS pattern: ofs://ozone1756774157/fid2/managed/hive/hdfs_db5
                ofs_pattern = r'ofs://[^/]+/([^/]+)/(raw|managed|work)/hive/([^/\s]+)'
                ofs_match = re.search(ofs_pattern, url)
                
                if ofs_match:
                    url_db_name = ofs_match.group(3).lower()
                    if url_db_name not in db_values:
                        db_values.append(url_db_name)
        
        # If no database values found (no database resource and no recognizable URL pattern),
        # exclude the policy when include_databases filter is specified
        if not db_values:
            # If user specified include_databases filter, exclude this policy
            if include_databases:
                return False
            # Otherwise, allow it through (for backward compatibility)
            return True
        
        if exclude_databases:
            exclude_list = [db.lower() for db in exclude_databases]
            if any(db in exclude_list for db in db_values):
                return False
        
        if include_databases:
            include_list = [db.lower() for db in include_databases]
            if not any(db in include_list for db in db_values):
                return False
        
        return True
    
    def filter_hive_policies(self, policies: List[Dict], include_databases: List[str] = None,
                            exclude_databases: List[str] = None) -> List[Dict]:
        """Filter out default policies and optionally filter by database names"""
        filtered = []
        for policy in policies:
            if self.is_default_policy(policy):
                self.logger.debug(f"Filtered out default policy: {policy.get('name')}")
                continue
            
            if not self.matches_database_filter(policy, include_databases, exclude_databases):
                continue
            
            filtered.append(policy)
        
        self.logger.info(f"Filtered {len(policies)} Hive policies down to {len(filtered)} policies")
        if include_databases:
            self.logger.info(f"Include databases: {', '.join(include_databases)}")
        if exclude_databases:
            self.logger.info(f"Exclude databases: {', '.join(exclude_databases)}")
        
        self.filtered_hive_policies = filtered
        return filtered
    
    def clone_hive_policy(self, policy: Dict, ozone_prefix: str, ozone_service_id: str) -> Dict:
        """Clone a Hive policy and transform it for Ozone with the specified prefix"""
        import copy
        cloned_policy = copy.deepcopy(policy)
        
        cloned_policy.pop('id', None)
        cloned_policy.pop('guid', None)
        cloned_policy.pop('version', None)
        
        original_name = cloned_policy.get('name', '')
        resources = cloned_policy.get('resources', {})
        new_name_parts = []
        
        self.logger.info(f"Cloning policy: {original_name}")
        self.logger.debug(f"Original resources: {list(resources.keys())}")
        
        # Transform database resource
        if 'database' in resources:
            database = resources['database']
            if 'values' in database:
                original_db_values = database['values']
                new_db_values = [f"{ozone_prefix}_{db}" for db in original_db_values]
                database['values'] = new_db_values
                new_name_parts.append(f"db={','.join(new_db_values)}")
                self.logger.debug(f"Transformed database: {original_db_values} -> {new_db_values}")
        
        # Transform URL resource
        if 'url' in resources:
            url_resource = resources['url']
            if 'values' in url_resource:
                original_urls = url_resource['values']
                new_urls = []
                
                for url in original_urls:
                    url = url.strip()
                    hdfs_pattern = r'hdfs://[^/]+/data/([^/]+)/(raw|managed|work)/hive/([^/\s]+)'
                    match = re.search(hdfs_pattern, url)
                    
                    if match:
                        fid = match.group(1)
                        layer = match.group(2)
                        db_name = match.group(3)
                        new_url = f"ofs://{ozone_service_id}/{fid}/{layer}/hive/{db_name}"
                        new_urls.append(new_url)
                        self.logger.debug(f"Transformed URL: {url} -> {new_url}")
                    else:
                        self.logger.warning(f"URL pattern not matched for: {url}")
                        new_url = url.replace('hdfs://', f'ofs://{ozone_service_id}/')
                        new_url = new_url.replace('/data/', '/')
                        new_urls.append(new_url)
                
                url_resource['values'] = new_urls
                if new_urls:
                    new_name_parts.append(f"url={new_urls[0]}")
        
        # Handle table resource (skip wildcards)
        if 'table' in resources:
            table_resource = resources['table']
            if 'values' in table_resource:
                table_values = table_resource['values']
                if table_values and table_values != ['*']:
                    new_name_parts.append(f"tbl={','.join(table_values)}")
                    self.logger.debug(f"Added table to name: tbl={','.join(table_values)}")
        
        # Handle column resource (skip wildcards - like table behavior)
        if 'column' in resources:
            column_resource = resources['column']
            self.logger.debug(f"Found column resource: {column_resource}")
            if 'values' in column_resource:
                column_values = column_resource['values']
                self.logger.debug(f"Column values: {column_values}")
                # Only include specific columns, skip wildcards
                if column_values and column_values != ['*']:
                    new_name_parts.append(f"col={','.join(column_values)}")
                    self.logger.info(f"Added column to name: col={','.join(column_values)}")
                else:
                    self.logger.debug(f"Skipping column wildcard in name")
            else:
                self.logger.debug(f"No column values found")
        else:
            self.logger.debug(f"No column resource found in policy")
        
        # Handle udf resource (keep wildcards)
        if 'udf' in resources:
            udf_resource = resources['udf']
            if 'values' in udf_resource:
                udf_values = udf_resource['values']
                if udf_values:
                    new_name_parts.append(f"udf={','.join(udf_values)}")
        
        # Build new policy name
        if new_name_parts:
            cloned_policy['name'] = ','.join(new_name_parts)
        else:
            cloned_policy['name'] = f"{ozone_prefix}_{original_name}"
        
        cloned_policy['description'] = f"Cloned from '{original_name}' for Ozone migration with prefix {ozone_prefix}"
        
        return cloned_policy
    
    def create_cloned_hive_policies(self, policies: List[Dict], ozone_prefix: str, ozone_service_id: str) -> List[Dict]:
        """Create cloned Hive policies for all filtered policies"""
        cloned = []
        
        self.logger.info(f"Creating cloned Hive policies with prefix: {ozone_prefix}")
        self.logger.info(f"Ozone service ID: {ozone_service_id}")
        
        for policy in policies:
            try:
                cloned_policy = self.clone_hive_policy(policy, ozone_prefix, ozone_service_id)
                cloned.append(cloned_policy)
                self.logger.info(f"Cloned Hive policy: {policy.get('name')} -> {cloned_policy.get('name')}")
            except Exception as e:
                self.logger.error(f"Failed to clone Hive policy {policy.get('name')}: {e}")
        
        self.logger.info(f"Successfully created {len(cloned)} cloned Hive policies")
        self.cloned_hive_policies = cloned
        return cloned
    
    # ==================== HDFS ACL FALLBACK METHODS ====================
    
    def kinit(self, keytab_path: str, principal: str) -> bool:
        """Perform Kerberos authentication"""
        try:
            subprocess.check_output(
                ['kinit', '-kt', keytab_path, principal],
                stderr=subprocess.STDOUT
            )
            self.logger.info(f"Kerberos authentication successful for {principal}")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"kinit failed: {e}")
            return False
        except FileNotFoundError:
            self.logger.error("kinit command not found. Please install Kerberos client.")
            return False
    
    def hdfs_path_exists(self, hdfs_path: str) -> bool:
        """Check if HDFS path exists"""
        if not self.ensure_kerberos_auth():
            return False
        
        try:
            subprocess.check_call(
                ["hadoop", "fs", "-test", "-e", hdfs_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return True
        except subprocess.CalledProcessError:
            return False
        except FileNotFoundError:
            self.logger.error("hadoop command not found. Please install HDFS client.")
            return False
    
    def get_hdfs_subdirs(self, hdfs_path: str) -> List[str]:
        """Get subdirectories under an HDFS path"""
        if not self.ensure_kerberos_auth():
            return []
        
        try:
            command = f"hdfs dfs -ls {hdfs_path} | grep '^d' | awk -F'/' '{{print $NF}}'"
            output = subprocess.check_output(command, shell=True, stderr=subprocess.DEVNULL)
            subdirs = output.decode('utf-8').strip().split('\n')
            return [d for d in subdirs if d]
        except subprocess.CalledProcessError:
            self.logger.warning(f"No subdirectories found or error accessing: {hdfs_path}")
            return []
        except FileNotFoundError:
            self.logger.error("hdfs command not found. Please install HDFS client.")
            return []
    
    def get_hdfs_facl(self, hdfs_path: str) -> str:
        """Get HDFS ACLs for a path"""
        if not self.ensure_kerberos_auth():
            return ""
        
        if not self.hdfs_path_exists(hdfs_path):
            self.logger.warning(f"HDFS path does not exist: {hdfs_path}")
            return ""
        
        try:
            output = subprocess.check_output(
                ["hdfs", "dfs", "-getfacl", hdfs_path],
                stderr=subprocess.PIPE
            )
            return output.decode('utf-8')
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to get ACLs for {hdfs_path}: {e}")
            return ""
        except FileNotFoundError:
            self.logger.error("hdfs command not found. Please install HDFS client.")
            return ""
    
    def ensure_kerberos_auth(self) -> bool:
        """Ensure Kerberos authentication is valid"""
        if 'KEYTAB_PATH' not in self.config or 'PRINCIPAL' not in self.config:
            return False
        
        # Check if we need to kinit (simple approach - always kinit)
        return self.kinit(self.config['KEYTAB_PATH'], self.config['PRINCIPAL'])
    
    def parse_hdfs_facl(self, facl_output: str) -> Tuple[List[str], List[str]]:
        """Parse HDFS ACL output into users and groups with read permissions"""
        users = []
        groups = []
        owner = ""
        primary_group = ""
        
        for line in facl_output.strip().splitlines():
            line = line.strip()
            
            if not line:
                continue
            
            # Parse owner
            if line.startswith("# owner:"):
                owner = line.split(":", 1)[1].strip()
            
            # Parse primary group
            elif line.startswith("# group:"):
                primary_group = line.split(":", 1)[1].strip()
            
            # Parse user permissions
            elif line.startswith("user:"):
                parts = line.split(":")
                if len(parts) >= 3:
                    user = parts[1]
                    perms = parts[2]
                    
                    # Check if user has read permission
                    if 'r' in perms:
                        if user:  # Specific user
                            if user not in users:
                                users.append(user)
                        elif owner:  # Owner permission (user::)
                            if owner not in users:
                                users.append(owner)
            
            # Parse group permissions
            elif line.startswith("group:"):
                parts = line.split(":")
                if len(parts) >= 3:
                    group = parts[1]
                    perms = parts[2]
                    
                    # Check if group has read permission
                    if 'r' in perms:
                        if group:  # Specific group
                            if group not in groups:
                                groups.append(group)
                        elif primary_group:  # Primary group permission (group::)
                            if primary_group not in groups:
                                groups.append(primary_group)
        
        return users, groups
    
    def parse_hdfs_facl_full_permissions(self, facl_output: str) -> List[Dict]:
        """Parse HDFS ACL output into full permission structure for key policies"""
        permission_items = []
        owner = ""
        primary_group = ""
        
        # First pass: get owner and primary group
        for line in facl_output.strip().splitlines():
            line = line.strip()
            if line.startswith("# owner:"):
                owner = line.split(":", 1)[1].strip()
            elif line.startswith("# group:"):
                primary_group = line.split(":", 1)[1].strip()
        
        # Track processed users and groups to avoid duplicates
        processed_users = set()
        processed_groups = set()
        
        # Second pass: parse permissions
        for line in facl_output.strip().splitlines():
            line = line.strip()
            
            if not line or line.startswith("#"):
                continue
            
            # Parse user permissions
            if line.startswith("user:"):
                parts = line.split(":")
                if len(parts) >= 3:
                    user = parts[1] if parts[1] else owner
                    perms = parts[2]
                    
                    if user and user not in processed_users:
                        accesses = self.convert_posix_to_ozone_permissions(perms)
                        if accesses:
                            permission_items.append({
                                'users': [user],
                                'groups': [],
                                'roles': [],
                                'accesses': accesses
                            })
                            processed_users.add(user)
            
            # Parse group permissions
            elif line.startswith("group:"):
                parts = line.split(":")
                if len(parts) >= 3:
                    group = parts[1] if parts[1] else primary_group
                    perms = parts[2]
                    
                    if group and group not in processed_groups:
                        accesses = self.convert_posix_to_ozone_permissions(perms)
                        if accesses:
                            permission_items.append({
                                'users': [],
                                'groups': [group],
                                'roles': [],
                                'accesses': accesses
                            })
                            processed_groups.add(group)
        
        return permission_items
    
    def convert_posix_to_ozone_permissions(self, posix_perms: str) -> List[Dict]:
        """Convert POSIX permissions (rwx) to Ozone permissions"""
        accesses = []
        has_read = 'r' in posix_perms
        has_write = 'w' in posix_perms
        has_execute = 'x' in posix_perms
        
        if has_read:
            accesses.append({'type': 'read', 'isAllowed': True})
        
        if has_write:
            accesses.append({'type': 'write', 'isAllowed': True})
        
        if has_read and has_execute:
            accesses.append({'type': 'list', 'isAllowed': True})
        
        if has_write and has_execute:
            accesses.append({'type': 'create', 'isAllowed': True})
            accesses.append({'type': 'delete', 'isAllowed': True})
        
        return accesses
    
    def create_ozone_policies_from_hdfs_acls(self, fid: str, ozone_service: str) -> List[Dict]:
        """Create Ozone policies from HDFS ACLs when no Ranger policies exist"""
        ozone_policies = []
        
        fid_path = f"{self.config.get('FID_DIR_PREFIX', '/data/')}{fid}"
        
        self.logger.info(f"Creating fallback policies from HDFS ACLs for FID: {fid}")
        
        # Get volume-level ACLs
        volume_facl = self.get_hdfs_facl(fid_path)
        
        if not volume_facl:
            self.logger.warning(f"No HDFS ACLs found for {fid_path}")
            return []
        
        # Parse volume-level ACLs for read permissions
        volume_users, volume_groups = self.parse_hdfs_facl(volume_facl)
        
        if not volume_users and not volume_groups:
            self.logger.warning(f"No read permissions found in HDFS ACLs for {fid_path}")
            return []
        
        # 1. Create Volume Policy
        volume_policy = OrderedDict()
        volume_policy['service'] = ozone_service
        volume_policy['name'] = f"{fid}_volume_policy_from_hdfs_acls"
        volume_policy['description'] = f"Created from HDFS ACLs for {fid_path}"
        volume_policy['resources'] = OrderedDict()
        volume_policy['resources']['volume'] = {'values': [fid]}
        volume_policy['policyItems'] = [{
            'accesses': [{'type': 'read', 'isAllowed': True}],
            'users': volume_users,
            'groups': volume_groups,
            'roles': []
        }]
        ozone_policies.append(volume_policy)
        self.logger.info(f"Created volume policy from HDFS ACLs: {volume_policy['name']}")
        
        # 2. Get bucket names (subdirectories)
        buckets = self.get_hdfs_subdirs(fid_path)
        
        if not buckets:
            self.logger.warning(f"No buckets (subdirectories) found under {fid_path}")
            # Still return volume policy
            return ozone_policies
        
        self.logger.info(f"Found buckets for {fid}: {', '.join(buckets)}")
        
        # 3. Create Bucket Policy
        bucket_policy = OrderedDict()
        bucket_policy['service'] = ozone_service
        bucket_policy['name'] = f"{fid}_bucket_policy_from_hdfs_acls"
        bucket_policy['description'] = f"Created from HDFS ACLs for buckets: {', '.join(buckets)}"
        bucket_policy['resources'] = OrderedDict()
        bucket_policy['resources']['volume'] = {'values': [fid]}
        bucket_policy['resources']['bucket'] = {'values': buckets}
        bucket_policy['policyItems'] = [{
            'accesses': [{'type': 'read', 'isAllowed': True}],
            'users': volume_users,
            'groups': volume_groups,
            'roles': []
        }]
        ozone_policies.append(bucket_policy)
        self.logger.info(f"Created bucket policy from HDFS ACLs: {bucket_policy['name']}")
        
        # 4. Create Key Policies for each bucket
        for bucket in buckets:
            bucket_path = f"{fid_path}/{bucket}"
            bucket_facl = self.get_hdfs_facl(bucket_path)
            
            if bucket_facl:
                # Parse full permissions for key policy
                permission_items = self.parse_hdfs_facl_full_permissions(bucket_facl)
                
                if permission_items:
                    key_policy = OrderedDict()
                    key_policy['service'] = ozone_service
                    key_policy['name'] = f"{fid}_{bucket}_key_policy_from_hdfs_acls"
                    key_policy['description'] = f"Created from HDFS ACLs for {bucket_path}"
                    key_policy['resources'] = OrderedDict()
                    key_policy['resources']['volume'] = {'values': [fid]}
                    key_policy['resources']['bucket'] = {'values': [bucket]}
                    key_policy['resources']['key'] = {'values': ['*'], 'isRecursive': True}
                    key_policy['policyItems'] = permission_items
                    ozone_policies.append(key_policy)
                    self.logger.info(f"Created key policy from HDFS ACLs: {key_policy['name']}")
        
        self.logger.info(f"Created {len(ozone_policies)} Ozone policies from HDFS ACLs for FID: {fid}")
        return ozone_policies
    
    def is_hdfs_acl_fallback_enabled(self) -> bool:
        """Check if HDFS ACL fallback is enabled"""
        enabled = self.config.get('ENABLE_HDFS_ACL_FALLBACK', 'false').lower() == 'true'
        
        if enabled:
            # Check for required parameters
            if 'KEYTAB_PATH' not in self.config:
                self.logger.error("HDFS ACL fallback enabled but KEYTAB_PATH not configured")
                return False
            if 'PRINCIPAL' not in self.config:
                self.logger.error("HDFS ACL fallback enabled but PRINCIPAL not configured")
                return False
            
            # Verify keytab file exists
            if not os.path.exists(self.config['KEYTAB_PATH']):
                self.logger.error(f"Keytab file not found: {self.config['KEYTAB_PATH']}")
                return False
        
        return enabled
    
    # ==================== HDFS POLICY CONVERSION METHODS ====================
    
    def extract_fid_from_path(self, hdfs_path: str) -> str:
        """Extract FID from HDFS path like /data/fid1/... -> fid1"""
        match = re.match(r'/data/([^/]+)', hdfs_path)
        return match.group(1) if match else None
    
    def get_fids_from_hdfs_policies(self, hdfs_policies: List[Dict], include_fids: List[str] = None,
                                    exclude_fids: List[str] = None) -> List[str]:
        """Extract unique FIDs from HDFS policies"""
        fids = set()
        
        for policy in hdfs_policies:
            resources = policy.get('resources', {})
            path = resources.get('path', {})
            paths = path.get('values', [])
            
            for hdfs_path in paths:
                fid = self.extract_fid_from_path(hdfs_path)
                if fid:
                    fids.add(fid)
        
        fid_list = list(fids)
        
        # Apply filters
        if exclude_fids:
            fid_list = [fid for fid in fid_list if fid not in exclude_fids]
        
        if include_fids:
            fid_list = [fid for fid in fid_list if fid in include_fids]
        
        self.logger.info(f"Found {len(fid_list)} FIDs to process: {', '.join(fid_list)}")
        return fid_list
    
    def get_hdfs_policies_for_fid(self, fid: str, hdfs_policies: List[Dict]) -> List[Dict]:
        """Get all HDFS policies related to a specific FID"""
        fid_dir = f"/data/{fid}"
        filtered = []
        
        for policy in hdfs_policies:
            resources = policy.get('resources', {})
            path = resources.get('path', {})
            paths = path.get('values', [])
            
            if any(p.startswith(fid_dir) for p in paths):
                filtered.append(policy)
        
        return filtered
    
    def categorize_hdfs_policies(self, hdfs_policies: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Categorize HDFS policies into volume/bucket/key levels"""
        volume_policies = []
        bucket_policies = []
        key_policies = []
        
        for policy in hdfs_policies:
            resources = policy.get('resources', {})
            paths = resources.get('path', {}).get('values', [])
            
            for path in paths:
                parts = path.rstrip('/').split('/')
                # ['', 'data', 'fid1'] = volume level (length 3)
                # ['', 'data', 'fid1', 'raw'] = bucket level (length 4)
                # ['', 'data', 'fid1', 'raw', 'key1'] = key level (length 5+)
                
                if len(parts) == 3:
                    volume_policies.append(policy)
                    break
                elif len(parts) == 4:
                    bucket_policies.append(policy)
                    break
                elif len(parts) >= 5:
                    key_policies.append(policy)
                    break
        
        return volume_policies, bucket_policies, key_policies
    
    def convert_permissions_hdfs_to_ozone(self, hdfs_accesses: List[Dict]) -> List[Dict]:
        """Convert HDFS permissions (read/write/execute) to Ozone (read/write/list/create/delete)"""
        ozone_accesses = []
        has_read = False
        has_write = False
        has_execute = False
        
        # Check what permissions exist
        for access in hdfs_accesses:
            if access.get('type') == 'read':
                has_read = True
                ozone_accesses.append({'type': 'read', 'isAllowed': True})
            elif access.get('type') == 'write':
                has_write = True
                ozone_accesses.append({'type': 'write', 'isAllowed': True})
            elif access.get('type') == 'execute':
                has_execute = True
        
        # Map combinations
        if has_read and has_execute:
            ozone_accesses.append({'type': 'list', 'isAllowed': True})
        
        if has_write and has_execute:
            ozone_accesses.append({'type': 'create', 'isAllowed': True})
            ozone_accesses.append({'type': 'delete', 'isAllowed': True})
        
        return ozone_accesses
    
    def create_ozone_volume_policy(self, fid: str, users: List[str], groups: List[str], 
                                   roles: List[str], ozone_service: str) -> Dict:
        """Create Ozone volume-level policy"""
        policy = OrderedDict()
        policy['service'] = ozone_service
        policy['name'] = f"{fid}_volume_policy"
        policy['resources'] = OrderedDict()
        policy['resources']['volume'] = {'values': [fid]}
        
        policy['policyItems'] = [{
            'accesses': [{'type': 'read', 'isAllowed': True}],
            'users': users,
            'groups': groups,
            'roles': roles
        }]
        
        return policy
    
    def create_ozone_bucket_policy(self, fid: str, buckets: List[str], users: List[str], 
                                   groups: List[str], roles: List[str], ozone_service: str) -> Dict:
        """Create Ozone bucket-level policy"""
        policy = OrderedDict()
        policy['service'] = ozone_service
        policy['name'] = f"{fid}_bucket_policy"
        policy['resources'] = OrderedDict()
        policy['resources']['volume'] = {'values': [fid]}
        policy['resources']['bucket'] = {'values': buckets}
        
        policy['policyItems'] = [{
            'accesses': [{'type': 'read', 'isAllowed': True}],
            'users': users,
            'groups': groups,
            'roles': roles
        }]
        
        return policy
    
    def create_ozone_key_policy(self, fid: str, bucket: str, key: str, hdfs_policy: Dict,
                               ozone_service: str) -> Dict:
        """Create Ozone key-level policy from HDFS policy"""
        policy = OrderedDict()
        policy['service'] = ozone_service
        policy['name'] = f"{fid}_{bucket}_{key}_key_policy"
        
        policy['resources'] = OrderedDict()
        policy['resources']['volume'] = {'values': [fid]}
        policy['resources']['bucket'] = {'values': [bucket]}
        policy['resources']['key'] = {'values': [key], 'isRecursive': True}
        
        # Convert policy items
        if 'policyItems' in hdfs_policy:
            ozone_policy_items = []
            for item in hdfs_policy['policyItems']:
                ozone_item = {
                    'accesses': self.convert_permissions_hdfs_to_ozone(item.get('accesses', [])),
                    'users': item.get('users', []),
                    'groups': item.get('groups', []),
                    'roles': item.get('roles', [])
                }
                ozone_policy_items.append(ozone_item)
            policy['policyItems'] = ozone_policy_items
        
        return policy
    
    def extract_bucket_and_key_from_path(self, hdfs_path: str, fid: str) -> Tuple[str, str]:
        """Extract bucket and key from HDFS path"""
        # /data/fid1/raw/key1/subkey -> bucket=raw, key=key1/subkey
        pattern = f'/data/{fid}/([^/]+)/(.*)'
        match = re.match(pattern, hdfs_path.rstrip('/'))
        
        if match:
            bucket = match.group(1)
            key = match.group(2) if match.group(2) else '*'
            return bucket, key
        
        return None, None
    
    def convert_hdfs_policies_for_fid(self, fid: str, hdfs_policies: List[Dict], 
                                     ozone_service: str) -> List[Dict]:
        """Convert all HDFS policies for a single FID to Ozone policies"""
        ozone_policies = []
        
        # Categorize policies
        volume_policies, bucket_policies, key_policies = self.categorize_hdfs_policies(hdfs_policies)
        
        # Collect all users/groups/roles from all policies
        all_users = set()
        all_groups = set()
        all_roles = set()
        
        for policy in hdfs_policies:
            for item in policy.get('policyItems', []):
                all_users.update(item.get('users', []))
                all_groups.update(item.get('groups', []))
                all_roles.update(item.get('roles', []))
        
        # Create volume policy
        if all_users or all_groups or all_roles:
            volume_policy = self.create_ozone_volume_policy(
                fid, list(all_users), list(all_groups), list(all_roles), ozone_service
            )
            ozone_policies.append(volume_policy)
        
        # Extract all buckets
        all_buckets = set()
        for policy in bucket_policies + key_policies:
            for path in policy.get('resources', {}).get('path', {}).get('values', []):
                bucket, _ = self.extract_bucket_and_key_from_path(path, fid)
                if bucket:
                    all_buckets.add(bucket)
        
        # Create bucket policy if buckets exist
        if all_buckets:
            bucket_policy = self.create_ozone_bucket_policy(
                fid, list(all_buckets), list(all_users), list(all_groups), list(all_roles), ozone_service
            )
            ozone_policies.append(bucket_policy)
        
        # Create key-level policies
        for policy in key_policies:
            for path in policy.get('resources', {}).get('path', {}).get('values', []):
                bucket, key = self.extract_bucket_and_key_from_path(path, fid)
                if bucket and key:
                    key_policy = self.create_ozone_key_policy(fid, bucket, key, policy, ozone_service)
                    ozone_policies.append(key_policy)
        
        self.logger.info(f"Converted {len(hdfs_policies)} HDFS policies to {len(ozone_policies)} Ozone policies for FID: {fid}")
        return ozone_policies
    
    def convert_all_hdfs_policies(self, hdfs_policies: List[Dict], ozone_service: str,
                                  include_fids: List[str] = None, exclude_fids: List[str] = None) -> List[Dict]:
        """Convert all HDFS policies to Ozone policies with ACL fallback support"""
        all_ozone_policies = []
        
        # Check if ACL fallback is enabled
        acl_fallback_enabled = self.is_hdfs_acl_fallback_enabled()
        if acl_fallback_enabled:
            self.logger.info("HDFS ACL fallback is ENABLED")
        else:
            self.logger.info("HDFS ACL fallback is DISABLED")
        
        # Get FIDs from Ranger policies
        ranger_fids = self.get_fids_from_hdfs_policies(hdfs_policies, include_fids, exclude_fids)
        
        # If include_fids specified but no Ranger policies found, still process them with ACL fallback
        fids_to_process = set(ranger_fids)
        
        if include_fids and acl_fallback_enabled:
            # Add explicitly requested FIDs that may not have Ranger policies
            fids_to_process.update(include_fids)
        
        if exclude_fids:
            # Remove excluded FIDs
            fids_to_process = fids_to_process - set(exclude_fids)
        
        fids_to_process = sorted(list(fids_to_process))
        self.logger.info(f"Total FIDs to process: {len(fids_to_process)}: {', '.join(fids_to_process)}")
        
        for fid in fids_to_process:
            self.logger.info(f"Processing FID: {fid}")
            fid_policies = self.get_hdfs_policies_for_fid(fid, hdfs_policies)
            
            if fid_policies:
                # Convert from Ranger policies
                ozone_policies = self.convert_hdfs_policies_for_fid(fid, fid_policies, ozone_service)
                all_ozone_policies.extend(ozone_policies)
                self.logger.info(f"Converted {len(ozone_policies)} Ozone policies from Ranger for FID: {fid}")
            else:
                # No Ranger policies found
                self.logger.warning(f"No HDFS Ranger policies found for FID: {fid}")
                
                # Try HDFS ACL fallback if enabled
                if acl_fallback_enabled:
                    self.logger.info(f"Attempting HDFS ACL fallback for FID: {fid}")
                    acl_policies = self.create_ozone_policies_from_hdfs_acls(fid, ozone_service)
                    
                    if acl_policies:
                        all_ozone_policies.extend(acl_policies)
                        self.logger.info(f"Created {len(acl_policies)} Ozone policies from HDFS ACLs for FID: {fid}")
                    else:
                        self.logger.warning(f"HDFS ACL fallback also failed for FID: {fid}")
                else:
                    self.logger.info(f"Skipping FID {fid} - no policies and ACL fallback disabled")
        
        self.logger.info(f"Total Ozone policies created: {len(all_ozone_policies)}")
        self.converted_ozone_policies = all_ozone_policies
        return all_ozone_policies
    
    # ==================== DISPLAY METHODS ====================
    
    def display_policies_summary(self, policies: List[Dict], title: str):
        """Display a summary of policies"""
        print(f"\n{'='*80}")
        print(f"{title}")
        print(f"{'='*80}")
        print(f"Total policies: {len(policies)}\n")
        
        for i, policy in enumerate(policies, 1):
            print(f"{i}. Policy Name: {policy.get('name')}")
            print(f"   Service: {policy.get('service')}")
            
            resources = policy.get('resources', {})
            
            # Display volume
            if 'volume' in resources:
                print(f"   Volume: {', '.join(resources['volume'].get('values', []))}")
            
            # Display bucket
            if 'bucket' in resources:
                print(f"   Bucket: {', '.join(resources['bucket'].get('values', []))}")
            
            # Display key
            if 'key' in resources:
                print(f"   Key: {', '.join(resources['key'].get('values', []))}")
            
            # Display database
            if 'database' in resources:
                db_values = resources['database'].get('values', [])
                print(f"   Database(s): {', '.join(db_values)}")
            
            # Display URL
            if 'url' in resources:
                url_values = resources['url'].get('values', [])
                print(f"   URL(s): {', '.join(url_values[:2])}")
                if len(url_values) > 2:
                    print(f"            ... and {len(url_values) - 2} more")
            
            # Display path
            if 'path' in resources:
                path_values = resources['path'].get('values', [])
                print(f"   Path(s): {', '.join(path_values[:2])}")
                if len(path_values) > 2:
                    print(f"            ... and {len(path_values) - 2} more")
            
            # Show policy items summary
            policy_items = policy.get('policyItems', [])
            if policy_items:
                groups = []
                users = []
                for item in policy_items:
                    groups.extend(item.get('groups', []))
                    users.extend(item.get('users', []))
                
                if groups:
                    print(f"   Groups: {', '.join(set(groups))}")
                if users:
                    print(f"   Users: {', '.join(set(users))}")
            
            print()


def main():
    parser = argparse.ArgumentParser(
        description='Ranger Policy Converter for HDFS to Ozone Migration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # SIMPLE USAGE (using config.ini for all settings)
  # Export Hive policies
  python3 ranger_policy_migration.py --config config.ini --mode hive --action export
  
  # Convert HDFS policies to Ozone
  python3 ranger_policy_migration.py --config config.ini --mode hdfs --action convert --save-json
  
  # Import both Hive and HDFS policies
  python3 ranger_policy_migration.py --config config.ini --mode both --action import --save-json
  
  # HIVE POLICY CLONING
  # Clone Hive policies for specific databases
  python3 ranger_policy_migration.py --config config.ini --mode hive --action import \\
    --databases "db1,db2" --save-json
  
  # Exclude test databases
  python3 ranger_policy_migration.py --config config.ini --mode hive --action import \\
    --exclude-databases "test_db,temp_db" --save-json
  
  # HDFS POLICY CONVERSION
  # Convert specific FIDs only
  python3 ranger_policy_migration.py --config config.ini --mode hdfs --action convert \\
    --fids "fid1,fid2,fid3" --save-json
  
  # Exclude specific FIDs
  python3 ranger_policy_migration.py --config config.ini --mode hdfs --action convert \\
    --exclude-fids "fid_old,fid_test" --save-json
  
  # HDFS ACL FALLBACK (when config.ini has ENABLE_HDFS_ACL_FALLBACK=true)
  # Just run normally - ACL fallback happens automatically
  python3 ranger_policy_migration.py --config config.ini --mode hdfs --action convert --save-json
  
  # Override config to enable ACL fallback for this run only
  python3 ranger_policy_migration.py --config config.ini --mode hdfs --action convert \\
    --enable-acl-fallback --save-json
  
  # Override config to disable ACL fallback for this run only
  python3 ranger_policy_migration.py --config config.ini --mode hdfs --action convert \\
    --disable-acl-fallback --save-json
  
  # OVERRIDE CONFIG VALUES VIA COMMAND LINE (if needed)
  # Override service names
  python3 ranger_policy_migration.py --config config.ini --mode hdfs --action convert \\
    --hdfs-service custom_hdfs --ozone-service custom_ozone
  
  # Override keytab and principal
  python3 ranger_policy_migration.py --config config.ini --mode hdfs --action convert \\
    --enable-acl-fallback \\
    --keytab /custom/path/hdfs.keytab \\
    --principal custom-hdfs@REALM.COM
  
  # COMBINED MODE
  python3 ranger_policy_migration.py --config config.ini --mode both --action import \\
    --databases "prod_db" --fids "fid1,fid2" --save-json
        """
    )
    
    parser.add_argument('--config', required=True, help='Path to config.ini file')
    parser.add_argument('--mode', required=True, choices=['hive', 'hdfs', 'both'],
                       help='Operation mode: hive (clone), hdfs (convert), or both')
    parser.add_argument('--action', required=True, 
                       choices=['export', 'filter', 'clone', 'convert', 'import', 'cleanup'],
                       help='Action to perform')
    #parser.add_argument('--action', required=True, 
    #                   choices=['export', 'filter', 'clone', 'convert', 'import'],
    #                   help='Action to perform')
    
    # Service names
    parser.add_argument('--hive-service', help='Ranger Hive service name (default: from config)')
    parser.add_argument('--hdfs-service', help='Ranger HDFS service name (default: from config)')
    parser.add_argument('--ozone-service', help='Ranger Ozone service name (default: from config)')
    
    # Ozone parameters
    parser.add_argument('--ozone-prefix', help='Prefix for Ozone databases (default: from config)')
    parser.add_argument('--ozone-service-id', help='Ozone service ID for ofs:// URLs (default: from config)')
    
    # Filtering options
    parser.add_argument('--databases', help='Comma-separated list of database names (Hive)')
    parser.add_argument('--databases-file', help='Path to file containing database names (one per line)')
    parser.add_argument('--exclude-databases', help='Comma-separated list of databases to exclude (Hive)')
    parser.add_argument('--exclude-databases-file', help='Path to file containing databases to exclude (one per line)')
    parser.add_argument('--fids', help='Comma-separated list of FIDs (HDFS)')
    parser.add_argument('--exclude-fids', help='Comma-separated list of FIDs to exclude (HDFS)')
    
    # HDFS ACL Fallback options (override config)
    parser.add_argument('--enable-acl-fallback', action='store_true',
                       help='Enable HDFS ACL fallback (overrides config)')
    parser.add_argument('--disable-acl-fallback', action='store_true',
                       help='Disable HDFS ACL fallback (overrides config)')
    parser.add_argument('--keytab', help='Path to Kerberos keytab file (overrides config)')
    parser.add_argument('--principal', help='Kerberos principal (overrides config)')
    
    # Output options
    parser.add_argument('--save-json', action='store_true', help='Save policies to JSON files')
    
    args = parser.parse_args()
    
    # Create converter instance
    converter = RangerPolicyConverter(args.config)
    
    # Get service names from config or command line (command line takes precedence)
    hive_service = args.hive_service or converter.config.get('HIVE_SERVICE', 'cm_hive')
    hdfs_service = args.hdfs_service or converter.config.get('HDFS_SERVICE', 'cm_hdfs')
    ozone_service = args.ozone_service or converter.config.get('OZONE_SERVICE', 'cm_ozone')
    
    # Get Ozone parameters from config or command line
    ozone_prefix = args.ozone_prefix or converter.config.get('OZONE_PREFIX')
    ozone_service_id = args.ozone_service_id or converter.config.get('OZONE_SERVICE_ID')
    
    # Validate mode-specific requirements
    if args.mode in ['hive', 'both']:
        if args.action in ['clone', 'import']:
            if not ozone_prefix:
                parser.error("--ozone-prefix required (or set OZONE_PREFIX in config.ini)")
            if not ozone_service_id:
                parser.error("--ozone-service-id required (or set OZONE_SERVICE_ID in config.ini)")
    
    if args.mode in ['hdfs', 'both']:
        if args.action in ['convert', 'import']:
            if not ozone_service:
                parser.error("--ozone-service required (or set OZONE_SERVICE in config.ini)")
    
    # Handle ACL fallback configuration
    acl_fallback_enabled = False
    
    # Check command-line override first
    if args.disable_acl_fallback:
        acl_fallback_enabled = False
        converter.logger.info("HDFS ACL fallback explicitly DISABLED via command line")
    elif args.enable_acl_fallback:
        acl_fallback_enabled = True
        converter.logger.info("HDFS ACL fallback explicitly ENABLED via command line")
    else:
        # Check config file
        acl_fallback_enabled = converter.config.get('ENABLE_HDFS_ACL_FALLBACK', 'false').lower() == 'true'
        if acl_fallback_enabled:
            converter.logger.info("HDFS ACL fallback ENABLED via config file")
        else:
            converter.logger.info("HDFS ACL fallback DISABLED (not enabled in config)")
    
    # Set ACL fallback in converter config
    converter.config['ENABLE_HDFS_ACL_FALLBACK'] = 'true' if acl_fallback_enabled else 'false'
    
    # Get keytab and principal (command line overrides config)
    if acl_fallback_enabled:
        keytab_path = args.keytab or converter.config.get('KEYTAB_PATH')
        principal = args.principal or converter.config.get('PRINCIPAL')
        
        if not keytab_path or not principal:
            converter.logger.error("ACL fallback enabled but KEYTAB_PATH or PRINCIPAL not configured")
            parser.error("ACL fallback requires --keytab and --principal (or KEYTAB_PATH and PRINCIPAL in config.ini)")
        
        # Update config with resolved values
        converter.config['KEYTAB_PATH'] = keytab_path
        converter.config['PRINCIPAL'] = principal
        
        converter.logger.info(f"  Keytab: {keytab_path}")
        converter.logger.info(f"  Principal: {principal}")
        
        # Verify keytab exists
        if not os.path.exists(keytab_path):
            converter.logger.error(f"Keytab file not found: {keytab_path}")
            parser.error(f"Keytab file not found: {keytab_path}")
    
    converter.logger.info(f"Service Configuration:")
    converter.logger.info(f"  Hive Service: {hive_service}")
    converter.logger.info(f"  HDFS Service: {hdfs_service}")
    converter.logger.info(f"  Ozone Service: {ozone_service}")
    if ozone_prefix:
        converter.logger.info(f"  Ozone Prefix: {ozone_prefix}")
    if ozone_service_id:
        converter.logger.info(f"  Ozone Service ID: {ozone_service_id}")
    
    # Parse filters
    include_databases = None
    exclude_databases = None
    
    # Handle databases - priority: command line > command line file > config file
    if args.databases:
        include_databases = [db.strip() for db in args.databases.split(',')]
        converter.logger.info(f"Using databases from command line: {len(include_databases)} databases")
    elif args.databases_file:
        include_databases = converter.load_list_from_file(args.databases_file)
        converter.logger.info(f"Using databases from file: {args.databases_file}")
    elif 'DATABASES_FILE' in converter.config:
        databases_file = converter.config['DATABASES_FILE']
        include_databases = converter.load_list_from_file(databases_file)
        converter.logger.info(f"Using databases from config file path: {databases_file}")
    
    # Handle exclude-databases - priority: command line > command line file > config file
    if args.exclude_databases:
        exclude_databases = [db.strip() for db in args.exclude_databases.split(',')]
        converter.logger.info(f"Using exclude-databases from command line: {len(exclude_databases)} databases")
    elif args.exclude_databases_file:
        exclude_databases = converter.load_list_from_file(args.exclude_databases_file)
        converter.logger.info(f"Using exclude-databases from file: {args.exclude_databases_file}")
    elif 'EXCLUDE_DATABASES_FILE' in converter.config:
        exclude_file = converter.config['EXCLUDE_DATABASES_FILE']
        exclude_databases = converter.load_list_from_file(exclude_file)
        converter.logger.info(f"Using exclude-databases from config file path: {exclude_file}")
    
    include_fids = [fid.strip() for fid in args.fids.split(',')] if args.fids else None
    exclude_fids = [fid.strip() for fid in args.exclude_fids.split(',')] if args.exclude_fids else None
    
    all_policies_to_import = []
    
    # ========== HIVE MODE ==========
    if args.mode in ['hive', 'both']:
        converter.logger.info("=== Starting Hive Policy Processing ===")
        
        # Export Hive policies
        hive_policies = converter.export_policies(hive_service, "Hive")
        
        if not hive_policies:
            converter.logger.error("No Hive policies exported")
            if args.mode == 'hive':
                sys.exit(1)
        else:
            if args.action == 'export':
                converter.display_policies_summary(hive_policies, "EXPORTED HIVE POLICIES")
                if args.save_json:
                    converter.save_policies_to_file(hive_policies, f"hive_exported_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            # Cleanup action
            if args.action == 'cleanup':
                converter.logger.info("=== Starting Hive Policy Cleanup ===")
                
                # Get policies to delete and keep
                policies_to_delete, policies_to_keep = converter.get_policies_for_cleanup(
                    hive_policies, include_databases
                )
                
                # Display cleanup summary
                print(f"\n{'='*80}")
                print("HIVE POLICY CLEANUP ANALYSIS")
                print(f"{'='*80}")
                print(f"Total policies in Ranger: {len(hive_policies)}")
                print(f"Policies to DELETE: {len(policies_to_delete)}")
                print(f"Policies to KEEP: {len(policies_to_keep)}")
                print(f"{'='*80}\n")
                
                if policies_to_delete:
                    # Show policies to be deleted
                    converter.display_policies_summary(policies_to_delete, "POLICIES TO BE DELETED")
                    
                    # Show policies to be kept
                    print(f"\n{'='*80}")
                    print(f"POLICIES TO BE KEPT ({len(policies_to_keep)} total)")
                    print(f"{'='*80}")
                    keep_reasons = {'default': 0, 'url': 0, 'other': 0}
                    for policy in policies_to_keep:
                        if converter.is_default_policy(policy):
                            keep_reasons['default'] += 1
                        elif converter.has_url_resource(policy):
                            keep_reasons['url'] += 1
                        else:
                            keep_reasons['other'] += 1
                    
                    print(f"  Default policies: {keep_reasons['default']}")
                    print(f"  URL policies: {keep_reasons['url']}")
                    print(f"  Other policies: {keep_reasons['other']}")
                    print()
                    
                    # Save to JSON if requested
                    if args.save_json:
                        converter.save_policies_to_file(
                            policies_to_delete, 
                            f"hive_cleanup_to_delete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        )
                        converter.save_policies_to_file(
                            policies_to_keep,
                            f"hive_cleanup_to_keep_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        )
                    
                    # Confirm deletion
                    print("\n" + "="*80)
                    print("WARNING: This action will permanently delete the policies listed above!")
                    print("="*80)
                    response = input(f"Delete {len(policies_to_delete)} policies from Ranger? (yes/no): ")
                    
                    if response.lower() in ['yes', 'y']:
                        results = converter.cleanup_hive_policies(policies_to_delete)
                        print(f"\nCleanup Results:")
                        print(f"  Successfully deleted: {results['success']}")
                        print(f"  Failed to delete: {results['failed']}")
                    else:
                        converter.logger.info("Cleanup cancelled by user")
                        print("Cleanup cancelled.")
                else:
                    print("\nNo policies match the cleanup criteria.")
                    converter.logger.info("No policies to cleanup")
            
            # Filter policies
            if args.action in ['filter', 'clone', 'convert', 'import']:
                filtered = converter.filter_hive_policies(hive_policies, include_databases, exclude_databases)
                
                if args.action == 'filter':
                    converter.display_policies_summary(filtered, "FILTERED HIVE POLICIES")
                    if args.save_json:
                        converter.save_policies_to_file(filtered, f"hive_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                
                # Clone policies
                if args.action in ['clone', 'convert', 'import']:
                    cloned = converter.create_cloned_hive_policies(filtered, ozone_prefix, ozone_service_id)
                    
                    if args.action in ['clone', 'convert']:
                        if cloned:
                            converter.display_policies_summary(cloned, f"CLONED HIVE POLICIES (PREFIX: {ozone_prefix})")
                        else:
                            print(f"\n{'='*80}")
                            print("NO HIVE POLICIES CREATED")
                            print(f"{'='*80}")
                            converter.logger.warning("No Hive policies were created")
                        
                        if args.save_json and cloned:
                            converter.save_policies_to_file(cloned, f"hive_cloned_{ozone_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                    
                    if args.action == 'import':
                        all_policies_to_import.extend(cloned)
    
    # ========== HDFS MODE ==========
    if args.mode in ['hdfs', 'both']:
        converter.logger.info("=== Starting HDFS Policy Processing ===")
        
        # Export HDFS policies
        hdfs_policies = converter.export_policies(hdfs_service, "HDFS")
        
        if not hdfs_policies:
            converter.logger.warning("No HDFS policies exported from Ranger")
            
            # If ACL fallback is enabled, continue anyway
            if not acl_fallback_enabled:
                converter.logger.error("No HDFS policies found and ACL fallback is disabled")
                if args.mode == 'hdfs':
                    sys.exit(1)
            else:
                converter.logger.info("ACL fallback is enabled - will attempt to create policies from filesystem ACLs")
                hdfs_policies = []  # Empty list to pass to convert function
        
        # Process policies (even if empty when ACL fallback enabled)
        if hdfs_policies or acl_fallback_enabled:
            if args.action == 'export' and hdfs_policies:
                converter.display_policies_summary(hdfs_policies, "EXPORTED HDFS POLICIES")
                if args.save_json:
                    converter.save_policies_to_file(hdfs_policies, f"hdfs_exported_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            # Convert HDFS policies (may use ACL fallback)
            if args.action in ['convert', 'import']:
                converted = converter.convert_all_hdfs_policies(hdfs_policies, ozone_service, include_fids, exclude_fids)
                
                if args.action == 'convert':
                    if converted:
                        converter.display_policies_summary(converted, "CONVERTED OZONE POLICIES FROM HDFS")
                    else:
                        print(f"\n{'='*80}")
                        print("NO OZONE POLICIES CREATED")
                        print(f"{'='*80}")
                        converter.logger.warning("No Ozone policies were created")
                    
                    if args.save_json and converted:
                        converter.save_policies_to_file(converted, f"hdfs_converted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                
                if args.action == 'import':
                    all_policies_to_import.extend(converted)
    
    # ========== IMPORT ==========
    if args.action == 'import' and all_policies_to_import:
        converter.display_policies_summary(all_policies_to_import, "POLICIES TO BE IMPORTED")
        
        print("\n" + "="*80)
        response = input(f"Import {len(all_policies_to_import)} policies to Ranger? (yes/no): ")
        
        if response.lower() in ['yes', 'y']:
            results = converter.import_policies(all_policies_to_import)
            print(f"\nImport Results:")
            print(f"  Successfully imported: {results['success']}")
            print(f"  Failed to import: {results['failed']}")
            
            if args.save_json:
                converter.save_policies_to_file(all_policies_to_import, f"imported_policies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        else:
            converter.logger.info("Import cancelled by user")
            print("Import cancelled.")


if __name__ == "__main__":
    main()
