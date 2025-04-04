import pandas as pd
import re
from typing import Dict, Any, List, Optional
import os

def process_natural_language_query(query: str, data: pd.DataFrame) -> str:
    """
    Process a natural language query and return the response.
    
    Args:
        query: Natural language query
        data: Dataframe containing arbitration data
        
    Returns:
        Response to the query
    """
    # Convert query to lowercase for easier matching
    query_lower = query.lower()
    
    # Check if the query is about an arbitrator's case count
    arbitrator_case_count_match = re.search(r'how many arbitrations? has ([^\?]+)', query_lower)
    if arbitrator_case_count_match:
        return _get_arbitrator_case_count(arbitrator_case_count_match.group(1), data)
    
    # Check if the query is about an arbitrator's rulings
    arbitrator_rulings_match = re.search(r'how many times has ([^?]+) ruled for the (complainant|consumer)', query_lower)
    if arbitrator_rulings_match:
        return _get_arbitrator_rulings(arbitrator_rulings_match.group(1), arbitrator_rulings_match.group(2), data)
    
    # Check if the query is about an arbitrator's average awards
    avg_award_match = re.search(r'what was the average award given by ([^\?]+)', query_lower)
    if avg_award_match:
        return _get_arbitrator_avg_award(avg_award_match.group(1), data)
    
    # Check if the query is about listing an arbitrator's cases
    list_cases_match = re.search(r'list the names of all the arbitrations? handled by ([^\?]+)', query_lower)
    if list_cases_match:
        return _list_arbitrator_cases(list_cases_match.group(1), data)
    
    # Check if the query is about an arbitrator's rulings against a specific respondent
    specific_rulings_match = re.search(r'how many times has ([^?]+) ruled for the consumer against ([^\?]+)', query_lower)
    if specific_rulings_match:
        return _get_specific_rulings(specific_rulings_match.group(1), specific_rulings_match.group(2), data)
    
    # If query doesn't match any of the patterns, provide a generic response
    return "I'm sorry, I couldn't understand your query. Please try rephrasing or use one of the sample queries below."

def _get_arbitrator_case_count(arbitrator_name: str, data: pd.DataFrame) -> str:
    """
    Get the case count for a specific arbitrator.
    
    Args:
        arbitrator_name: Name of the arbitrator (from query)
        data: Dataframe containing arbitration data
        
    Returns:
        Response string with the case count
    """
    # Clean the arbitrator name
    arbitrator_name = arbitrator_name.strip()
    
    # Try to find the arbitrator in the data
    matching_arbitrators = data['Arbitrator_Name'].str.lower().str.contains(arbitrator_name.lower())
    
    if matching_arbitrators.any():
        # Get the first matching arbitrator name (with proper case)
        actual_name = data.loc[matching_arbitrators, 'Arbitrator_Name'].iloc[0]
        
        # Count cases
        case_count = matching_arbitrators.sum()
        
        return f"Arbitrator {actual_name} has handled {case_count} arbitration cases in the dataset."
    else:
        return f"I couldn't find any cases for an arbitrator matching '{arbitrator_name}' in the dataset."

def _get_arbitrator_rulings(arbitrator_name: str, party: str, data: pd.DataFrame) -> str:
    """
    Get the number of times an arbitrator ruled for a specific party.
    
    Args:
        arbitrator_name: Name of the arbitrator (from query)
        party: Party type (complainant or consumer)
        data: Dataframe containing arbitration data
        
    Returns:
        Response string with the ruling count
    """
    # Clean the arbitrator name
    arbitrator_name = arbitrator_name.strip()
    
    # Try to find the arbitrator in the data
    matching_arbitrators = data['Arbitrator_Name'].str.lower().str.contains(arbitrator_name.lower())
    
    if matching_arbitrators.any():
        # Get the first matching arbitrator name (with proper case)
        actual_name = data.loc[matching_arbitrators, 'Arbitrator_Name'].iloc[0]
        
        # Filter for the arbitrator
        arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
        
        # Count awarded cases (simplified logic - actual implementation would depend on data structure)
        # This assumes "Awarded" disposition indicates a ruling for the consumer/complainant
        awarded_cases = arbitrator_data[arbitrator_data['Disposition_Type'] == 'Awarded'].shape[0]
        
        return f"Arbitrator {actual_name} has ruled for the {party} in {awarded_cases} cases out of {len(arbitrator_data)} total cases."
    else:
        return f"I couldn't find any cases for an arbitrator matching '{arbitrator_name}' in the dataset."

def _get_arbitrator_avg_award(arbitrator_name: str, data: pd.DataFrame) -> str:
    """
    Get the average award amount for a specific arbitrator.
    
    Args:
        arbitrator_name: Name of the arbitrator (from query)
        data: Dataframe containing arbitration data
        
    Returns:
        Response string with the average award amount
    """
    # Clean the arbitrator name
    arbitrator_name = arbitrator_name.strip()
    
    # Try to find the arbitrator in the data
    matching_arbitrators = data['Arbitrator_Name'].str.lower().str.contains(arbitrator_name.lower())
    
    if matching_arbitrators.any():
        # Get the first matching arbitrator name (with proper case)
        actual_name = data.loc[matching_arbitrators, 'Arbitrator_Name'].iloc[0]
        
        # Filter for the arbitrator
        arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
        
        # Check if award data is available
        if 'Award_Amount' in arbitrator_data.columns and not arbitrator_data['Award_Amount'].isna().all():
            # Calculate average award
            avg_award = arbitrator_data['Award_Amount'].mean()
            
            return f"The average award given by Arbitrator {actual_name} is ${avg_award:,.2f}."
        else:
            return f"Award amount data is not available for Arbitrator {actual_name}."
    else:
        return f"I couldn't find any cases for an arbitrator matching '{arbitrator_name}' in the dataset."

def _list_arbitrator_cases(arbitrator_name: str, data: pd.DataFrame) -> str:
    """
    List the cases handled by a specific arbitrator.
    
    Args:
        arbitrator_name: Name of the arbitrator (from query)
        data: Dataframe containing arbitration data
        
    Returns:
        Response string with the case list
    """
    # Clean the arbitrator name
    arbitrator_name = arbitrator_name.strip()
    
    # Try to find the arbitrator in the data
    matching_arbitrators = data['Arbitrator_Name'].str.lower().str.contains(arbitrator_name.lower())
    
    if matching_arbitrators.any():
        # Get the first matching arbitrator name (with proper case)
        actual_name = data.loc[matching_arbitrators, 'Arbitrator_Name'].iloc[0]
        
        # Filter for the arbitrator
        arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
        
        # Get case names
        if 'Case_Name' in arbitrator_data.columns:
            case_names = arbitrator_data['Case_Name'].tolist()
            
            # Format the response
            if len(case_names) > 0:
                case_list = "\n".join([f"- {name}" for name in case_names])
                return f"Arbitrator {actual_name} has handled the following {len(case_names)} cases:\n\n{case_list}"
            else:
                return f"No case names are available for Arbitrator {actual_name}."
        else:
            return f"Case name data is not available for Arbitrator {actual_name}."
    else:
        return f"I couldn't find any cases for an arbitrator matching '{arbitrator_name}' in the dataset."

def _get_specific_rulings(arbitrator_name: str, respondent_name: str, data: pd.DataFrame) -> str:
    """
    Get the number of times an arbitrator ruled for the consumer against a specific respondent.
    
    Args:
        arbitrator_name: Name of the arbitrator (from query)
        respondent_name: Name of the respondent (from query)
        data: Dataframe containing arbitration data
        
    Returns:
        Response string with the ruling count
    """
    # Clean the names
    arbitrator_name = arbitrator_name.strip()
    respondent_name = respondent_name.strip()
    
    # Try to find the arbitrator in the data
    matching_arbitrators = data['Arbitrator_Name'].str.lower().str.contains(arbitrator_name.lower())
    
    if matching_arbitrators.any():
        # Get the first matching arbitrator name (with proper case)
        actual_arb_name = data.loc[matching_arbitrators, 'Arbitrator_Name'].iloc[0]
        
        # Try to find the respondent in the data
        matching_respondents = data['Respondent_Name'].str.lower().str.contains(respondent_name.lower())
        
        if matching_respondents.any():
            # Get the first matching respondent name (with proper case)
            actual_resp_name = data.loc[matching_respondents, 'Respondent_Name'].iloc[0]
            
            # Filter for the arbitrator and respondent
            filtered_data = data[(data['Arbitrator_Name'] == actual_arb_name) & 
                                (data['Respondent_Name'] == actual_resp_name)]
            
            # Count awarded cases (simplified logic - actual implementation would depend on data structure)
            # This assumes "Awarded" disposition indicates a ruling for the consumer
            awarded_cases = filtered_data[filtered_data['Disposition_Type'] == 'Awarded'].shape[0]
            
            return f"Arbitrator {actual_arb_name} has ruled for the consumer against {actual_resp_name} in {awarded_cases} cases out of {len(filtered_data)} total cases involving both parties."
        else:
            return f"I couldn't find any cases for a respondent matching '{respondent_name}' in the dataset."
    else:
        return f"I couldn't find any cases for an arbitrator matching '{arbitrator_name}' in the dataset."
