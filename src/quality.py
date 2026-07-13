"""
Data quality checks and validation module for AWS Lakehouse Data Platform
"""
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Perform data quality checks on datasets"""
    
    def __init__(self, df: pd.DataFrame, table_name: str = ""):
        self.df = df
        self.table_name = table_name
        self.checks_passed = []
        self.checks_failed = []
    
    def check_null_values(self, columns: List[str] = None, threshold: float = 0.0) -> Dict:
        """Check for null values"""
        if columns is None:
            columns = self.df.columns.tolist()
        
        results = {}
        for col in columns:
            null_count = self.df[col].isnull().sum()
            null_percentage = (null_count / len(self.df)) * 100
            
            passed = null_percentage <= threshold
            results[col] = {
                "null_count": int(null_count),
                "null_percentage": round(null_percentage, 2),
                "passed": passed
            }
            
            if passed:
                self.checks_passed.append(f"Null check passed for {col}")
            else:
                self.checks_failed.append(f"Null check failed for {col}: {null_percentage}%")
        
        return results
    
    def check_duplicates(self, subset: List[str] = None) -> Dict:
        """Check for duplicate rows"""
        duplicates = self.df.duplicated(subset=subset).sum()
        total_rows = len(self.df)
        duplicate_percentage = (duplicates / total_rows) * 100 if total_rows > 0 else 0
        
        result = {
            "duplicate_rows": int(duplicates),
            "duplicate_percentage": round(duplicate_percentage, 2),
            "total_rows": total_rows,
            "passed": duplicates == 0
        }
        
        if result["passed"]:
            self.checks_passed.append("Duplicate check passed")
        else:
            self.checks_failed.append(f"Duplicate check failed: {duplicates} duplicates found")
        
        return result
    
    def check_data_types(self, expected_types: Dict[str, str]) -> Dict:
        """Check if columns have expected data types"""
        results = {}
        for col, expected_type in expected_types.items():
            if col not in self.df.columns:
                results[col] = {"exists": False, "passed": False}
                self.checks_failed.append(f"Column {col} not found")
                continue
            
            actual_type = str(self.df[col].dtype)
            passed = expected_type.lower() in actual_type.lower()
            results[col] = {
                "expected_type": expected_type,
                "actual_type": actual_type,
                "passed": passed
            }
            
            if passed:
                self.checks_passed.append(f"Data type check passed for {col}")
            else:
                self.checks_failed.append(f"Data type mismatch for {col}: expected {expected_type}, got {actual_type}")
        
        return results
    
    def check_value_range(self, column: str, min_value: float = None, max_value: float = None) -> Dict:
        """Check if values are within expected range"""
        if column not in self.df.columns:
            return {"error": f"Column {column} not found"}
        
        col_data = self.df[column].dropna()
        violations = 0
        
        if min_value is not None:
            violations += (col_data < min_value).sum()
        if max_value is not None:
            violations += (col_data > max_value).sum()
        
        violation_percentage = (violations / len(col_data)) * 100 if len(col_data) > 0 else 0
        passed = violations == 0
        
        result = {
            "column": column,
            "min_value": min_value,
            "max_value": max_value,
            "violations": int(violations),
            "violation_percentage": round(violation_percentage, 2),
            "passed": passed
        }
        
        if passed:
            self.checks_passed.append(f"Range check passed for {column}")
        else:
            self.checks_failed.append(f"Range check failed for {column}: {violations} violations")
        
        return result
    
    def get_summary(self) -> Dict:
        """Get summary of all quality checks"""
        total_checks = len(self.checks_passed) + len(self.checks_failed)
        passed_count = len(self.checks_passed)
        failed_count = len(self.checks_failed)
        
        return {
            "table_name": self.table_name,
            "total_checks": total_checks,
            "passed": passed_count,
            "failed": failed_count,
            "success_rate": round((passed_count / total_checks) * 100, 2) if total_checks > 0 else 0,
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed
        }


def validate_dataset(df: pd.DataFrame, rules: Dict) -> Tuple[bool, Dict]:
    """
    Validate dataset against a set of rules
    
    Args:
        df: DataFrame to validate
        rules: Dictionary of validation rules
    
    Returns:
        Tuple of (is_valid, results_summary)
    """
    checker = DataQualityChecker(df)
    
    # Apply null checks
    if "null_checks" in rules:
        checker.check_null_values(rules["null_checks"].get("columns"), 
                                  rules["null_checks"].get("threshold", 0))
    
    # Apply duplicate checks
    if "duplicate_checks" in rules:
        checker.check_duplicates(rules["duplicate_checks"].get("subset"))
    
    # Apply data type checks
    if "data_type_checks" in rules:
        checker.check_data_types(rules["data_type_checks"])
    
    # Apply value range checks
    if "range_checks" in rules:
        for range_check in rules["range_checks"]:
            checker.check_value_range(
                range_check["column"],
                range_check.get("min_value"),
                range_check.get("max_value")
            )
    
    summary = checker.get_summary()
    is_valid = summary["failed"] == 0
    
    return is_valid, summary
