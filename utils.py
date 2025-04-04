import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, date

def filter_dataframe(
    df: pd.DataFrame,
    arbitrator: Optional[str] = None,
    respondent: Optional[str] = None,
    attorney: Optional[str] = None,
    forum: Optional[str] = None,
    disposition: Optional[str] = None,
    date_range: Optional[List[Union[datetime, date]]] = None
) -> pd.DataFrame:
    """
    Filter the dataframe based on provided criteria.
    
    Args:
        df: Dataframe to filter
        arbitrator: Arbitrator name filter
        respondent: Respondent name filter
        attorney: Consumer attorney filter
        forum: Arbitration forum filter
        disposition: Disposition type filter
        date_range: Date range filter [start_date, end_date]
        
    Returns:
        Filtered dataframe
    """
    # Make a copy to avoid modifying the original
    filtered_df = df.copy()
    
    # Apply filters if provided
    if arbitrator:
        filtered_df = filtered_df[filtered_df['Arbitrator_Name'] == arbitrator]
    
    if respondent:
        filtered_df = filtered_df[filtered_df['Respondent_Name'] == respondent]
    
    if attorney:
        filtered_df = filtered_df[filtered_df['Consumer_Attorney'] == attorney]
    
    if forum:
        filtered_df = filtered_df[filtered_df['Forum'] == forum]
    
    if disposition:
        filtered_df = filtered_df[filtered_df['Disposition_Type'] == disposition]
    
    if date_range and len(date_range) == 2 and 'Date_Filed' in filtered_df.columns:
        start_date, end_date = date_range
        # Convert Python date objects to pandas datetime64 for comparison
        start_date_pd = pd.to_datetime(start_date)
        end_date_pd = pd.to_datetime(end_date)
        filtered_df = filtered_df[(filtered_df['Date_Filed'] >= start_date_pd) & 
                                  (filtered_df['Date_Filed'] <= end_date_pd)]
    
    return filtered_df

def format_currency(value: Union[float, int, None]) -> str:
    """
    Format a value as currency.
    
    Args:
        value: Value to format
        
    Returns:
        Formatted currency string
    """
    if value is None or pd.isna(value):
        return "$0.00"
    
    return f"${value:,.2f}"

def format_date(value: Union[datetime, date, None]) -> str:
    """
    Format a date value.
    
    Args:
        value: Date to format
        
    Returns:
        Formatted date string
    """
    if value is None or pd.isna(value):
        return ""
    
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    
    return str(value)

def calculate_date_diff(
    start_date: Union[datetime, date, None],
    end_date: Union[datetime, date, None]
) -> Optional[int]:
    """
    Calculate the difference in days between two dates.
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Difference in days, or None if dates are invalid
    """
    if start_date is None or end_date is None or pd.isna(start_date) or pd.isna(end_date):
        return None
    
    # Convert to pandas datetime objects for consistent handling
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Calculate difference in days
    diff = (end_dt - start_dt).days
    
    return diff

def get_top_items(data: Dict[str, int], n: int = 10) -> Dict[str, int]:
    """
    Get the top N items from a dictionary by value.
    
    Args:
        data: Dictionary of items and counts
        n: Number of top items to return
        
    Returns:
        Dictionary with top N items
    """
    return dict(sorted(data.items(), key=lambda x: x[1], reverse=True)[:n])

def convert_to_title_case(text: str) -> str:
    """
    Convert text to title case, preserving common abbreviations.
    
    Args:
        text: Text to convert
        
    Returns:
        Title-cased text
    """
    if not text:
        return text
    
    # Common abbreviations to preserve
    abbreviations = ['LLC', 'Inc.', 'Ltd.', 'Corp.', 'AAA', 'JAMS']
    
    # Split text into words
    words = text.split()
    
    # Convert each word
    for i, word in enumerate(words):
        # Check if word is an abbreviation
        if word.upper() in abbreviations:
            words[i] = word.upper()
        else:
            words[i] = word.capitalize()
    
    return ' '.join(words)
