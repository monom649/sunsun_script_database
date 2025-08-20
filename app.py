#!/usr/bin/env python3
"""
Secure Google Sheets CLI application.

Provides secure command-line interface for reading Google Sheets data
with watch mode, filtering, and health checking capabilities.
"""

import sys
import json
import time
import random
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

from config import load_config, check_environment
from logging_conf import setup_logging, get_logger, log_config_summary, log_watch_cycle
from extractor import (
    SheetsExtractor, 
    ExtractionError, 
    AuthenticationError, 
    NetworkError, 
    DataError,
    create_status_filter,
    create_date_range_filter,
    create_text_search_filter,
    create_multi_value_filter
)


def parse_filter_string(filter_str: str) -> Dict[str, str]:
    """Parse filter string into key-value pairs."""
    filters = {}
    if not filter_str:
        return filters
    
    # Simple parsing: key=value,key2=value2
    for part in filter_str.split(','):
        if '=' in part:
            key, value = part.split('=', 1)
            filters[key.strip()] = value.strip()
    
    return filters


def create_filter_function(filters: Dict[str, str]):
    """Create a composite filter function from filter dictionary."""
    if not filters:
        return None
    
    def combined_filter(record: Dict) -> bool:
        for key, value in filters.items():
            if key == 'status':
                if not create_status_filter(value)(record):
                    return False
            elif key.startswith('date_'):
                # Handle date filters: date_after, date_before
                date_column = key.replace('date_after_', '').replace('date_before_', '')
                if 'after' in key:
                    if not create_date_range_filter(date_column, start_date=value)(record):
                        return False
                elif 'before' in key:
                    if not create_date_range_filter(date_column, end_date=value)(record):
                        return False
            elif key.endswith('_contains'):
                # Text search: column_contains=search_term
                column = key.replace('_contains', '')
                if not create_text_search_filter(column, value)(record):
                    return False
            elif key.endswith('_in'):
                # Multi-value filter: column_in=value1|value2|value3
                column = key.replace('_in', '')
                values = value.split('|')
                if not create_multi_value_filter(column, values)(record):
                    return False
            else:
                # Exact match
                if record.get(key, '') != value:
                    return False
        
        return True
    
    return combined_filter


def run_extraction(args) -> int:
    """Run single data extraction."""
    logger = get_logger()
    
    try:
        # Load configuration
        config = load_config()
        log_config_summary(config.validate_sheet_access())
        
        # Initialize extractor
        extractor = SheetsExtractor(config)
        
        # Parse filters
        filters = parse_filter_string(args.filter) if args.filter else {}
        filter_func = create_filter_function(filters)
        
        # Parse columns
        columns = [col.strip() for col in args.columns.split(',')] if args.columns else None
        
        # Extract data
        records = extractor.extract_data(
            columns=columns,
            filter_func=filter_func,
            limit=args.limit
        )
        
        # Format output
        output_data = {
            'timestamp': datetime.now().isoformat(),
            'count': len(records),
            'records': records
        }
        
        # Output results
        if args.out:
            output_path = Path(args.out)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Results saved to {args.out}")
        else:
            print(json.dumps(output_data, indent=2, ensure_ascii=False))
        
        extractor.close()
        return 0
        
    except AuthenticationError as e:
        logger.error(f"Authentication failed: {e}")
        return 3
    except (NetworkError, DataError) as e:
        logger.error(f"Operation failed: {e}")
        return 4
    except ExtractionError as e:
        logger.error(f"Extraction error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def run_watch_mode(args) -> int:
    """Run continuous watch mode."""
    logger = get_logger()
    
    try:
        # Load configuration
        config = load_config()
        log_config_summary(config.validate_sheet_access())
        
        # Initialize extractor
        extractor = SheetsExtractor(config)
        
        # Parse filters
        filters = parse_filter_string(args.filter) if args.filter else {}
        filter_func = create_filter_function(filters)
        
        # Parse columns
        columns = [col.strip() for col in args.columns.split(',')] if args.columns else None
        
        logger.info(f"Starting watch mode with {args.interval}s interval")
        
        while True:
            try:
                # Calculate next run time with jitter
                jitter_factor = 1 + random.uniform(-config.jitter_percent/100, config.jitter_percent/100)
                actual_interval = int(args.interval * jitter_factor)
                next_run = datetime.now() + timedelta(seconds=actual_interval)
                
                log_watch_cycle(actual_interval, next_run.strftime('%Y-%m-%d %H:%M:%S'))
                
                # Extract data
                records = extractor.extract_data(
                    columns=columns,
                    filter_func=filter_func,
                    limit=args.limit
                )
                
                # Save results if output path specified
                if args.out:
                    output_data = {
                        'timestamp': datetime.now().isoformat(),
                        'count': len(records),
                        'records': records
                    }
                    
                    output_path = Path(args.out)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(output_data, f, indent=2, ensure_ascii=False)
                
                log_watch_cycle(actual_interval, next_run.strftime('%Y-%m-%d %H:%M:%S'), len(records))
                
                # Sleep until next iteration
                time.sleep(actual_interval)
                
            except AuthenticationError as e:
                logger.error(f"Authentication error in watch mode: {e}")
                return 3
            except KeyboardInterrupt:
                logger.info("Watch mode stopped by user")
                break
            except (NetworkError, DataError, ExtractionError) as e:
                logger.warning(f"Watch cycle failed, continuing: {e}")
                time.sleep(actual_interval)
            except Exception as e:
                logger.error(f"Unexpected error in watch mode: {e}")
                time.sleep(actual_interval)
        
        extractor.close()
        return 0
        
    except Exception as e:
        logger.error(f"Failed to start watch mode: {e}")
        return 1


def run_health_check(args) -> int:
    """Run health check."""
    logger = get_logger()
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize extractor
        extractor = SheetsExtractor(config)
        
        # Perform health check
        health_status = extractor.health_check()
        
        # Output results
        if args.out:
            output_path = Path(args.out)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(health_status, f, indent=2, ensure_ascii=False)
        else:
            print(json.dumps(health_status, indent=2, ensure_ascii=False))
        
        extractor.close()
        
        # Return appropriate exit code based on health status
        if health_status['status'] == 'healthy':
            return 0
        elif health_status['status'] == 'auth_error':
            return 3
        else:
            return 4
            
    except AuthenticationError as e:
        logger.error(f"Health check authentication failed: {e}")
        return 3
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Secure Google Sheets data extractor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s run --filter 'status=PUBLISHED' --limit 100
  %(prog)s run --columns 'title,status,date' --out results.json
  %(prog)s watch --interval 300 --filter 'status=ACTIVE'
  %(prog)s health
        """
    )
    
    # Global options
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Logging level')
    parser.add_argument('--log-file', help='Log file path')
    parser.add_argument('--no-console', action='store_true', help='Disable console logging')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Extract data once')
    run_parser.add_argument('--filter', help='Filter criteria (key=value,key2=value2)')
    run_parser.add_argument('--columns', help='Comma-separated column names')
    run_parser.add_argument('--limit', type=int, help='Maximum number of records')
    run_parser.add_argument('--out', help='Output file path')
    
    # Watch command
    watch_parser = subparsers.add_parser('watch', help='Continuous monitoring mode')
    watch_parser.add_argument('--interval', type=int, default=300, 
                             help='Check interval in seconds (default: 300)')
    watch_parser.add_argument('--filter', help='Filter criteria (key=value,key2=value2)')
    watch_parser.add_argument('--columns', help='Comma-separated column names')
    watch_parser.add_argument('--limit', type=int, help='Maximum number of records')
    watch_parser.add_argument('--out', help='Output file path (updated each cycle)')
    
    # Health command
    health_parser = subparsers.add_parser('health', help='Check system health')
    health_parser.add_argument('--out', help='Output file path')
    
    args = parser.parse_args()
    
    # Check if environment is configured
    if not check_environment():
        print("Environment not properly configured. Please check your .env file.", file=sys.stderr)
        return 2
    
    # Setup logging
    setup_logging(
        level=args.log_level,
        log_file=args.log_file,
        enable_console=not args.no_console
    )
    
    # Execute command
    if args.command == 'run':
        return run_extraction(args)
    elif args.command == 'watch':
        return run_watch_mode(args)
    elif args.command == 'health':
        return run_health_check(args)
    else:
        parser.print_help()
        return 2


if __name__ == '__main__':
    sys.exit(main())