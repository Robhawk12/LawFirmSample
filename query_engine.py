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
    arbitrator_case_count_match = re.search(r'how many arbitrations? has (arbitrator )?(.*?)(\?|$|had)', query_lower)
    if arbitrator_case_count_match:
        return _get_arbitrator_case_count(arbitrator_case_count_match.group(2).strip(), data)
    
    # Check if the query is about an arbitrator's rulings
    arbitrator_rulings_match = re.search(r'how many times has (arbitrator )?(.*?) ruled for the (complainant|consumer)', query_lower)
    if arbitrator_rulings_match:
        return _get_arbitrator_rulings(arbitrator_rulings_match.group(2).strip(), arbitrator_rulings_match.group(3), data)
    
    # Check if the query is about an arbitrator's average awards
    avg_award_match = re.search(r'what was the average award given by (arbitrator )?(.*?)(\?|$)', query_lower)
    if avg_award_match:
        return _get_arbitrator_avg_award(avg_award_match.group(2).strip(), data)
    
    # Check if the query is about listing an arbitrator's cases
    list_cases_match = re.search(r'list the names of all( the)? arbitrations? handled by (arbitrator )?(.*?)(\?|$|\.)', query_lower)
    if list_cases_match:
        return _list_arbitrator_cases(list_cases_match.group(3).strip(), data)
    
    # Check if the query is about an arbitrator's rulings against a specific respondent
    specific_rulings_match = re.search(r'how many times has (arbitrator )?(.*?) ruled for the consumer against (.*?)(\?|$|\.)', query_lower)
    if specific_rulings_match:
        return _get_specific_rulings(specific_rulings_match.group(2).strip(), specific_rulings_match.group(3).strip(), data)
    
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
    # Clean the arbitrator name and remove any trailing "had" or other words
    arbitrator_name = arbitrator_name.strip().rstrip('had').strip()
    
    # Try an exact match first (case-insensitive)
    exact_match = data['Arbitrator_Name'].str.lower() == arbitrator_name.lower()
    if exact_match.any():
        actual_name = data.loc[exact_match, 'Arbitrator_Name'].iloc[0]
        case_count = exact_match.sum()
        return f"Arbitrator {actual_name} has handled {case_count} arbitration cases in the dataset."
    
    # Try a partial match if exact match fails
    matching_arbitrators = data['Arbitrator_Name'].str.lower().str.contains(arbitrator_name.lower())
    
    if matching_arbitrators.any():
        # Get the first matching arbitrator name (with proper case)
        actual_name = data.loc[matching_arbitrators, 'Arbitrator_Name'].iloc[0]
        
        # Count cases for this specific arbitrator (exact match on the full name)
        case_count = (data['Arbitrator_Name'] == actual_name).sum()
        
        return f"Arbitrator {actual_name} has handled {case_count} arbitration cases in the dataset."
    else:
        # Try a more flexible match by splitting the name into parts
        name_parts = arbitrator_name.lower().split()
        for name_part in name_parts:
            if len(name_part) > 2:  # Only use parts that are meaningful (longer than 2 chars)
                flexible_match = data['Arbitrator_Name'].str.lower().str.contains(name_part)
                if flexible_match.any():
                    actual_name = data.loc[flexible_match, 'Arbitrator_Name'].iloc[0]
                    case_count = (data['Arbitrator_Name'] == actual_name).sum()
                    return f"Arbitrator {actual_name} has handled {case_count} arbitration cases in the dataset."
        
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
    
    # Try an exact match first (case-insensitive)
    exact_match = data['Arbitrator_Name'].str.lower() == arbitrator_name.lower()
    if exact_match.any():
        actual_name = data.loc[exact_match, 'Arbitrator_Name'].iloc[0]
        # Filter for exact match
        arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
        # Count awarded cases
        awarded_cases = arbitrator_data[arbitrator_data['Disposition_Type'] == 'Awarded'].shape[0]
        return f"Arbitrator {actual_name} has ruled for the {party} in {awarded_cases} cases out of {len(arbitrator_data)} total cases."
    
    # Try a partial match if exact match fails
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
        # Try a more flexible match by splitting the name into parts
        name_parts = arbitrator_name.lower().split()
        for name_part in name_parts:
            if len(name_part) > 2:  # Only use parts that are meaningful (longer than 2 chars)
                flexible_match = data['Arbitrator_Name'].str.lower().str.contains(name_part)
                if flexible_match.any():
                    actual_name = data.loc[flexible_match, 'Arbitrator_Name'].iloc[0]
                    arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
                    awarded_cases = arbitrator_data[arbitrator_data['Disposition_Type'] == 'Awarded'].shape[0]
                    return f"Arbitrator {actual_name} has ruled for the {party} in {awarded_cases} cases out of {len(arbitrator_data)} total cases."
        
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
    
    # Try an exact match first (case-insensitive)
    exact_match = data['Arbitrator_Name'].str.lower() == arbitrator_name.lower()
    if exact_match.any():
        actual_name = data.loc[exact_match, 'Arbitrator_Name'].iloc[0]
        # Filter for exact match
        arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
        
        # Check if award data is available
        if 'Award_Amount' in arbitrator_data.columns and not arbitrator_data['Award_Amount'].isna().all():
            # Calculate average award
            avg_award = arbitrator_data['Award_Amount'].mean()
            
            return f"The average award given by Arbitrator {actual_name} is ${avg_award:,.2f}."
        else:
            return f"Award amount data is not available for Arbitrator {actual_name}."
    
    # Try a partial match if exact match fails
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
        # Try a more flexible match by splitting the name into parts
        name_parts = arbitrator_name.lower().split()
        for name_part in name_parts:
            if len(name_part) > 2:  # Only use parts that are meaningful (longer than 2 chars)
                flexible_match = data['Arbitrator_Name'].str.lower().str.contains(name_part)
                if flexible_match.any():
                    actual_name = data.loc[flexible_match, 'Arbitrator_Name'].iloc[0]
                    arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
                    
                    # Check if award data is available
                    if 'Award_Amount' in arbitrator_data.columns and not arbitrator_data['Award_Amount'].isna().all():
                        # Calculate average award
                        avg_award = arbitrator_data['Award_Amount'].mean()
                        
                        return f"The average award given by Arbitrator {actual_name} is ${avg_award:,.2f}."
                    else:
                        return f"Award amount data is not available for Arbitrator {actual_name}."
        
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
    
    # Try an exact match first (case-insensitive)
    exact_match = data['Arbitrator_Name'].str.lower() == arbitrator_name.lower()
    if exact_match.any():
        actual_name = data.loc[exact_match, 'Arbitrator_Name'].iloc[0]
        # Filter for exact match
        arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
        
        # Get case IDs and respondent names for better identification
        if len(arbitrator_data) > 0:
            case_ids = arbitrator_data['Case_ID'].tolist()
            
            # Get respondent names if available for better context
            if 'Respondent_Name' in arbitrator_data.columns:
                respondents = arbitrator_data['Respondent_Name'].tolist()
                case_info = []
                
                for i, case_id in enumerate(case_ids):
                    # Add respondent info if available
                    if i < len(respondents) and pd.notna(respondents[i]):
                        case_info.append(f"- Case {case_id} against {respondents[i]}")
                    else:
                        case_info.append(f"- Case {case_id}")
                
                case_list = "\n".join(case_info)
                return f"Arbitrator {actual_name} has handled the following {len(case_ids)} cases:\n\n{case_list}"
            else:
                # Format with just case IDs
                case_list = "\n".join([f"- Case {case_id}" for case_id in case_ids])
                return f"Arbitrator {actual_name} has handled the following {len(case_ids)} cases:\n\n{case_list}"
        else:
            return f"No cases are available for Arbitrator {actual_name}."
    
    # Try a partial match if exact match fails
    matching_arbitrators = data['Arbitrator_Name'].str.lower().str.contains(arbitrator_name.lower())
    
    if matching_arbitrators.any():
        # Get the first matching arbitrator name (with proper case)
        actual_name = data.loc[matching_arbitrators, 'Arbitrator_Name'].iloc[0]
        
        # Filter for the arbitrator
        arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
        
        # Process case data
        if len(arbitrator_data) > 0:
            case_ids = arbitrator_data['Case_ID'].tolist()
            
            # Get respondent names if available
            if 'Respondent_Name' in arbitrator_data.columns:
                respondents = arbitrator_data['Respondent_Name'].tolist()
                case_info = []
                
                for i, case_id in enumerate(case_ids):
                    # Add respondent info if available
                    if i < len(respondents) and pd.notna(respondents[i]):
                        case_info.append(f"- Case {case_id} against {respondents[i]}")
                    else:
                        case_info.append(f"- Case {case_id}")
                
                case_list = "\n".join(case_info)
                return f"Arbitrator {actual_name} has handled the following {len(case_ids)} cases:\n\n{case_list}"
            else:
                # Format with just case IDs
                case_list = "\n".join([f"- Case {case_id}" for case_id in case_ids])
                return f"Arbitrator {actual_name} has handled the following {len(case_ids)} cases:\n\n{case_list}"
        else:
            return f"No cases are available for Arbitrator {actual_name}."
    else:
        # Try a more flexible match by splitting the name into parts
        name_parts = arbitrator_name.lower().split()
        for name_part in name_parts:
            if len(name_part) > 2:  # Only use parts that are meaningful (longer than 2 chars)
                flexible_match = data['Arbitrator_Name'].str.lower().str.contains(name_part)
                if flexible_match.any():
                    actual_name = data.loc[flexible_match, 'Arbitrator_Name'].iloc[0]
                    arbitrator_data = data[data['Arbitrator_Name'] == actual_name]
                    
                    # Process case data
                    if len(arbitrator_data) > 0:
                        case_ids = arbitrator_data['Case_ID'].tolist()
                        
                        # Get respondent names if available
                        if 'Respondent_Name' in arbitrator_data.columns:
                            respondents = arbitrator_data['Respondent_Name'].tolist()
                            case_info = []
                            
                            for i, case_id in enumerate(case_ids):
                                # Add respondent info if available
                                if i < len(respondents) and pd.notna(respondents[i]):
                                    case_info.append(f"- Case {case_id} against {respondents[i]}")
                                else:
                                    case_info.append(f"- Case {case_id}")
                            
                            case_list = "\n".join(case_info)
                            return f"Arbitrator {actual_name} has handled the following {len(case_ids)} cases:\n\n{case_list}"
                        else:
                            # Format with just case IDs
                            case_list = "\n".join([f"- Case {case_id}" for case_id in case_ids])
                            return f"Arbitrator {actual_name} has handled the following {len(case_ids)} cases:\n\n{case_list}"
                    else:
                        return f"No cases are available for Arbitrator {actual_name}."
        
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
    
    # Find the arbitrator using the improved matching approach
    # Try an exact match first (case-insensitive)
    arb_exact_match = data['Arbitrator_Name'].str.lower() == arbitrator_name.lower()
    if arb_exact_match.any():
        actual_arb_name = data.loc[arb_exact_match, 'Arbitrator_Name'].iloc[0]
    else:
        # Try a partial match if exact match fails
        arb_partial_match = data['Arbitrator_Name'].str.lower().str.contains(arbitrator_name.lower())
        if arb_partial_match.any():
            actual_arb_name = data.loc[arb_partial_match, 'Arbitrator_Name'].iloc[0]
        else:
            # Try a more flexible match by splitting the name into parts
            arb_found = False
            name_parts = arbitrator_name.lower().split()
            for name_part in name_parts:
                if len(name_part) > 2:  # Only use parts that are meaningful (longer than 2 chars)
                    arb_flexible_match = data['Arbitrator_Name'].str.lower().str.contains(name_part)
                    if arb_flexible_match.any():
                        actual_arb_name = data.loc[arb_flexible_match, 'Arbitrator_Name'].iloc[0]
                        arb_found = True
                        break
            
            if not arb_found:
                return f"I couldn't find any cases for an arbitrator matching '{arbitrator_name}' in the dataset."
    
    # Find the respondent using the improved matching approach
    # Try an exact match first (case-insensitive)
    resp_exact_match = data['Respondent_Name'].str.lower() == respondent_name.lower()
    if resp_exact_match.any():
        actual_resp_name = data.loc[resp_exact_match, 'Respondent_Name'].iloc[0]
    else:
        # Try a partial match if exact match fails
        resp_partial_match = data['Respondent_Name'].str.lower().str.contains(respondent_name.lower())
        if resp_partial_match.any():
            actual_resp_name = data.loc[resp_partial_match, 'Respondent_Name'].iloc[0]
        else:
            # Try a more flexible match by splitting the name into parts
            resp_found = False
            name_parts = respondent_name.lower().split()
            for name_part in name_parts:
                if len(name_part) > 2:  # Only use parts that are meaningful (longer than 2 chars)
                    resp_flexible_match = data['Respondent_Name'].str.lower().str.contains(name_part)
                    if resp_flexible_match.any():
                        actual_resp_name = data.loc[resp_flexible_match, 'Respondent_Name'].iloc[0]
                        resp_found = True
                        break
            
            if not resp_found:
                return f"I couldn't find any cases for a respondent matching '{respondent_name}' in the dataset."
    
    # Filter for the arbitrator and respondent
    filtered_data = data[(data['Arbitrator_Name'] == actual_arb_name) & 
                        (data['Respondent_Name'] == actual_resp_name)]
    
    # Count awarded cases (simplified logic - actual implementation would depend on data structure)
    # This assumes "Awarded" disposition indicates a ruling for the consumer
    awarded_cases = filtered_data[filtered_data['Disposition_Type'] == 'Awarded'].shape[0]
    
    return f"Arbitrator {actual_arb_name} has ruled for the consumer against {actual_resp_name} in {awarded_cases} cases out of {len(filtered_data)} total cases involving both parties."
