import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple

class DataAnalyzer:
    """Class for analyzing arbitration data."""
    
    def __init__(self, data: pd.DataFrame):
        """
        Initialize the analyzer with data.
        
        Args:
            data: Dataframe containing arbitration data
        """
        self.data = data
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """
        Calculate key metrics from the data.
        
        Returns:
            Dictionary of metrics
        """
        metrics = {}
        
        # Total disputes
        metrics['total_disputes'] = len(self.data)
        
        # Total claim amount
        if 'Claim_Amount' in self.data.columns:
            total_claim = self.data['Claim_Amount'].sum()
            metrics['total_claim_amount'] = total_claim if not pd.isna(total_claim) else 0
        else:
            metrics['total_claim_amount'] = 0
        
        # Average claim amount
        if 'Claim_Amount' in self.data.columns and not self.data['Claim_Amount'].isna().all():
            avg_claim = self.data['Claim_Amount'].mean()
            metrics['avg_claim_amount'] = avg_claim if not pd.isna(avg_claim) else 0
        else:
            metrics['avg_claim_amount'] = 0
        
        # Average consumer claimant amount
        metrics['avg_consumer_claimant'] = self._calculate_avg_consumer_claim()
        
        # Cases by disposition type
        metrics['disposition_counts'] = self._count_by_column('Disposition_Type')
        
        # Cases by arbitrator
        metrics['arbitrator_counts'] = self._count_by_column('Arbitrator_Name')
        
        # Cases by respondent
        metrics['respondent_counts'] = self._count_by_column('Respondent_Name')
        
        # Cases by consumer attorney
        metrics['attorney_counts'] = self._count_by_column('Consumer_Attorney')
        
        # Cases by forum
        metrics['forum_counts'] = self._count_by_column('Forum')
        
        # Calculate settle rate
        metrics['settlement_rate'] = self._calculate_settlement_rate()
        
        return metrics
    
    def _calculate_avg_consumer_claim(self) -> float:
        """
        Calculate the average consumer claim amount.
        
        Returns:
            Average consumer claim amount
        """
        if 'Claim_Amount' not in self.data.columns:
            return 0
        
        # Filter for consumer claims if possible
        # This is a simplification - actual logic would depend on data structure
        consumer_claims = self.data['Claim_Amount']
        
        if consumer_claims.empty or consumer_claims.isna().all():
            return 0
        
        avg_claim = consumer_claims.mean()
        return avg_claim if not pd.isna(avg_claim) else 0
    
    def _count_by_column(self, column: str) -> Dict[str, int]:
        """
        Count occurrences by column value.
        
        Args:
            column: Column to count by
            
        Returns:
            Dictionary of counts by value
        """
        if column not in self.data.columns:
            return {}
        
        counts = self.data[column].value_counts().to_dict()
        
        # Remove None/NaN if present
        if None in counts:
            del counts[None]
        if np.nan in counts:
            del counts[np.nan]
        if 'Unknown' in counts and counts['Unknown'] == 0:
            del counts['Unknown']
        
        return counts
    
    def _calculate_settlement_rate(self) -> float:
        """
        Calculate the settlement rate.
        
        Returns:
            Settlement rate as a percentage
        """
        if 'Disposition_Type' not in self.data.columns:
            return 0.0
        
        total_cases = len(self.data)
        if total_cases == 0:
            return 0.0
        
        # Count settled cases
        settled_cases = self.data[self.data['Disposition_Type'].str.contains('Settled', case=False, na=False)].shape[0]
        
        return (settled_cases / total_cases) * 100
    
    def get_top_arbitrators(self, n: int = 10) -> List[Tuple[str, int]]:
        """
        Get the top N arbitrators by case count.
        
        Args:
            n: Number of arbitrators to return
            
        Returns:
            List of (arbitrator_name, case_count) tuples
        """
        if 'Arbitrator_Name' not in self.data.columns:
            return []
        
        # Count cases by arbitrator
        arbitrator_counts = self.data['Arbitrator_Name'].value_counts()
        
        # Convert to list of tuples
        top_arbitrators = [(name, count) for name, count in arbitrator_counts.items()]
        
        # Sort by count (descending) and take top N
        top_arbitrators.sort(key=lambda x: x[1], reverse=True)
        
        return top_arbitrators[:n]
    
    def get_top_respondents(self, n: int = 10) -> List[Tuple[str, int]]:
        """
        Get the top N respondents by case count.
        
        Args:
            n: Number of respondents to return
            
        Returns:
            List of (respondent_name, case_count) tuples
        """
        if 'Respondent_Name' not in self.data.columns:
            return []
        
        # Count cases by respondent
        respondent_counts = self.data['Respondent_Name'].value_counts()
        
        # Convert to list of tuples
        top_respondents = [(name, count) for name, count in respondent_counts.items()]
        
        # Sort by count (descending) and take top N
        top_respondents.sort(key=lambda x: x[1], reverse=True)
        
        return top_respondents[:n]
    
    def get_disposition_distribution(self) -> Dict[str, int]:
        """
        Get the distribution of case dispositions.
        
        Returns:
            Dictionary of {disposition_type: count}
        """
        if 'Disposition_Type' not in self.data.columns:
            return {}
        
        # Count cases by disposition type
        disposition_counts = self.data['Disposition_Type'].value_counts().to_dict()
        
        # Remove None/NaN if present
        if None in disposition_counts:
            del disposition_counts[None]
        if np.nan in disposition_counts:
            del disposition_counts[np.nan]
        if 'Unknown' in disposition_counts and disposition_counts['Unknown'] == 0:
            del disposition_counts['Unknown']
        
        return disposition_counts
    
    def calculate_arbitrator_statistics(self, arbitrator_name: str) -> Dict[str, Any]:
        """
        Calculate statistics for a specific arbitrator.
        
        Args:
            arbitrator_name: Name of the arbitrator
            
        Returns:
            Dictionary of statistics
        """
        if 'Arbitrator_Name' not in self.data.columns:
            return {}
        
        # Filter for the specified arbitrator
        arbitrator_data = self.data[self.data['Arbitrator_Name'] == arbitrator_name]
        
        if arbitrator_data.empty:
            return {}
        
        stats = {}
        
        # Basic counts
        stats['total_cases'] = len(arbitrator_data)
        
        # Disposition distribution
        if 'Disposition_Type' in arbitrator_data.columns:
            stats['disposition_counts'] = arbitrator_data['Disposition_Type'].value_counts().to_dict()
        
        # Average claim amount
        if 'Claim_Amount' in arbitrator_data.columns and not arbitrator_data['Claim_Amount'].isna().all():
            stats['avg_claim_amount'] = arbitrator_data['Claim_Amount'].mean()
        
        # Average award amount
        if 'Award_Amount' in arbitrator_data.columns and not arbitrator_data['Award_Amount'].isna().all():
            stats['avg_award_amount'] = arbitrator_data['Award_Amount'].mean()
        
        # Respondent companies
        if 'Respondent_Name' in arbitrator_data.columns:
            stats['respondent_counts'] = arbitrator_data['Respondent_Name'].value_counts().to_dict()
        
        return stats
