import pandas as pd
import numpy as np
import re
from typing import List, Dict, Any, Optional, Union, Tuple
import os
from database import ArbitrationDatabase

class DataProcessor:
    """Class for processing arbitration data from Excel files."""
    
    def __init__(self):
        # Field mappings for different sources
        self.field_mappings = {
            'AAA': {
                'case_id': ['Case ID', 'Case Number', 'Case No.', 'Case_ID'],
                'case_name': ['Case Name', 'Case_Name'],
                'arbitrator_name': ['Arbitrator Name', 'Arbitrator', 'Arbitrator_Name'],
                'respondent_name': ['Respondent Name', 'Respondent', 'Company', 'Business', 'Respondent_Name'],
                'consumer_attorney': ['Consumer Attorney', 'Claimant Attorney', 'Consumer_Attorney', 'Consumer Attorney Name'],
                'respondent_attorney': ['Respondent Attorney', 'Company Attorney', 'Respondent_Attorney'],
                'disposition_type': ['Type of Disposition', 'Disposition', 'Case Outcome', 'Disposition_Type'],
                'date_filed': ['Date Filed', 'Filing Date', 'Date_Filed'],
                'date_closed': ['Date Closed', 'Closing Date', 'Date_Closed'],
                'award_amount': ['Award Amount', 'Award', 'Award_Amount'],
                'claim_amount': ['Claim Amount', 'Demand Amount', 'Claim_Amount']
            },
            'JAMS': {
                'case_id': ['Case ID', 'Case #', 'Case Number', 'Reference Number', 'Case_ID'],
                'case_name': ['Case Name', 'Case Title', 'Case_Name'],
                'arbitrator_name': ['Arbitrator Name', 'Arbitrator', 'Neutral', 'Arbitrator_Name'],
                'respondent_name': ['Respondent Name', 'Respondent', 'Company', 'Business', 'Respondent_Name'],
                'consumer_attorney': ['Consumer Attorney', 'Claimant Attorney', 'Consumer_Attorney', 'Consumer Attorney Name'],
                'respondent_attorney': ['Respondent Attorney', 'Company Attorney', 'Respondent_Attorney'],
                'disposition_type': ['Type of Disposition', 'Disposition', 'Case Outcome', 'Result', 'Disposition_Type'],
                'date_filed': ['Date Filed', 'Filing Date', 'Date_Filed', 'Date of Filing'],
                'date_closed': ['Date Closed', 'Closing Date', 'Date_Closed', 'Date of Resolution'],
                'award_amount': ['Award Amount', 'Award', 'Award_Amount'],
                'claim_amount': ['Claim Amount', 'Demand Amount', 'Claim_Amount']
            }
        }
        
        # Standard column names for output
        self.standard_columns = [
            'Case_ID', 'Case_Name', 'Arbitrator_Name', 'Respondent_Name',
            'Consumer_Attorney', 'Respondent_Attorney', 'Disposition_Type',
            'Date_Filed', 'Date_Closed', 'Award_Amount', 'Claim_Amount', 'Forum'
        ]
    
    def process_files(self, file_paths: List[str], save_to_db: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Process all Excel files and combine them into a single dataframe.
        Optionally save to the database.
        
        Args:
            file_paths: List of file paths to Excel files
            save_to_db: Whether to save the processed data to the database
            
        Returns:
            Tuple containing:
                - Combined and processed dataframe
                - Dictionary with database operation results (if save_to_db is True)
        """
        all_dataframes = []
        db_result = {"status": "not_saved", "message": "Data was not saved to database"}
        
        for file_path in file_paths:
            try:
                # Determine source (AAA or JAMS) from filename
                filename = os.path.basename(file_path).lower()
                if 'aaa' in filename:
                    source = 'AAA'
                elif 'jams' in filename:
                    source = 'JAMS'
                else:
                    # If source cannot be determined from filename, try to infer from content
                    source = self._infer_source_from_content(file_path)
                
                # Read the Excel file
                df = pd.read_excel(file_path, engine='openpyxl')
                
                # Process the dataframe
                processed_df = self._process_dataframe(df, source)
                
                # Add to list of dataframes
                all_dataframes.append(processed_df)
                
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
        
        # Combine all dataframes
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Handle duplicates
            combined_df = self._handle_duplicates(combined_df)
            
            # Final cleaning
            combined_df = self._final_cleaning(combined_df)
            
            # Calculate consumer_prevailed and business_prevailed flags
            combined_df = self._calculate_prevailed_flags(combined_df)
            
            # Calculate case duration in days
            combined_df = self._calculate_case_duration(combined_df)
            
            # Save to database if requested
            if save_to_db:
                try:
                    db = ArbitrationDatabase()
                    db_result = db.save_data(combined_df)
                except Exception as e:
                    db_result = {"status": "error", "message": f"Database error: {str(e)}"}
            
            return combined_df, db_result
        
        return pd.DataFrame(), db_result
    
    def load_from_database(self, filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Load arbitration data from the database.
        
        Args:
            filters: Dictionary of database filters to apply
            
        Returns:
            DataFrame containing the loaded data
        """
        try:
            db = ArbitrationDatabase()
            df = db.load_data(filters)
            return df
        except Exception as e:
            print(f"Error loading data from database: {e}")
            return pd.DataFrame()
    
    def _calculate_prevailed_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate flags for who prevailed in each case.
        
        Args:
            df: DataFrame to process
            
        Returns:
            DataFrame with consumer_prevailed and business_prevailed flags
        """
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Initialize columns if they don't exist
        if 'Consumer_Prevailed' not in df.columns:
            df['Consumer_Prevailed'] = False
        if 'Business_Prevailed' not in df.columns:
            df['Business_Prevailed'] = False
        
        # Set flags based on disposition type and award amount
        if 'Disposition_Type' in df.columns:
            # Cases where consumer clearly prevailed
            consumer_prevailed_mask = (
                (df['Disposition_Type'].str.contains('Award', case=False, na=False)) & 
                (df['Award_Amount'] > 0)
            )
            df.loc[consumer_prevailed_mask, 'Consumer_Prevailed'] = True
            
            # Cases where business clearly prevailed
            business_prevailed_mask = (
                ((df['Disposition_Type'].str.contains('Dismiss', case=False, na=False)) |
                 (df['Disposition_Type'].str.contains('Withdrawn', case=False, na=False))) |
                ((df['Disposition_Type'].str.contains('Award', case=False, na=False)) & 
                 (df['Award_Amount'] == 0))
            )
            df.loc[business_prevailed_mask, 'Business_Prevailed'] = True
            
            # For settlements, neither party clearly prevailed
            settlement_mask = df['Disposition_Type'].str.contains('Settle', case=False, na=False)
            df.loc[settlement_mask, ['Consumer_Prevailed', 'Business_Prevailed']] = False
        
        return df
    
    def _calculate_case_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the duration of each case in days.
        
        Args:
            df: DataFrame to process
            
        Returns:
            DataFrame with case_duration_days column
        """
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Initialize column if it doesn't exist
        if 'Case_Duration_Days' not in df.columns:
            df['Case_Duration_Days'] = None
        
        # Calculate duration for cases with both dates
        if 'Date_Filed' in df.columns and 'Date_Closed' in df.columns:
            # Only calculate for rows with valid dates
            valid_dates_mask = (~pd.isna(df['Date_Filed'])) & (~pd.isna(df['Date_Closed']))
            
            # Calculate duration in days
            df.loc[valid_dates_mask, 'Case_Duration_Days'] = (
                df.loc[valid_dates_mask, 'Date_Closed'] - 
                df.loc[valid_dates_mask, 'Date_Filed']
            ).dt.days
        
        return df
    
    def _infer_source_from_content(self, file_path: str) -> str:
        """
        Infer the source (AAA or JAMS) from the file content.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            Source type: 'AAA' or 'JAMS'
        """
        try:
            # Read just the first few rows to check for source indicators
            df = pd.read_excel(file_path, nrows=5, engine='openpyxl')
            
            # Check column names for source indicators
            columns_str = ' '.join(df.columns.astype(str).tolist()).lower()
            
            if 'aaa' in columns_str or 'american arbitration' in columns_str:
                return 'AAA'
            elif 'jams' in columns_str:
                return 'JAMS'
            
            # Check cell values for source indicators
            values_str = ' '.join(df.values.astype(str).flatten()).lower()
            
            if 'aaa' in values_str or 'american arbitration' in values_str:
                return 'AAA'
            elif 'jams' in values_str:
                return 'JAMS'
            
            # Default to AAA if cannot determine
            return 'AAA'
            
        except Exception as e:
            print(f"Error inferring source from content: {e}")
            # Default to AAA if cannot determine
            return 'AAA'
    
    def _map_column(self, df: pd.DataFrame, field_type: str, source: str) -> Optional[str]:
        """
        Find the column in the dataframe that matches the field type.
        
        Args:
            df: Dataframe to search in
            field_type: Type of field to find (e.g., 'case_id')
            source: Source of the data ('AAA' or 'JAMS')
            
        Returns:
            Column name if found, None otherwise
        """
        possible_names = self.field_mappings[source][field_type]
        existing_columns = df.columns.tolist()
        
        # Check for exact matches
        for name in possible_names:
            if name in existing_columns:
                return name
        
        # Check for case-insensitive matches
        existing_columns_lower = [col.lower() for col in existing_columns]
        for name in possible_names:
            if name.lower() in existing_columns_lower:
                idx = existing_columns_lower.index(name.lower())
                return existing_columns[idx]
        
        # Check for partial matches
        for name in possible_names:
            for col in existing_columns:
                if (name.lower() in col.lower() or col.lower() in name.lower()) and \
                   (len(name) >= 4 or len(col) >= 4):  # Avoid matching too short strings
                    return col
        
        return None
    
    def _process_dataframe(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        Process a single dataframe from an Excel file.
        
        Args:
            df: Dataframe to process
            source: Source of the data ('AAA' or 'JAMS')
            
        Returns:
            Processed dataframe
        """
        # Create a new dataframe with standard columns
        processed_df = pd.DataFrame(columns=self.standard_columns)
        
        # Map each field type to a column in the original dataframe
        for field_type, standard_col in [
            ('case_id', 'Case_ID'),
            ('case_name', 'Case_Name'),
            ('arbitrator_name', 'Arbitrator_Name'),
            ('respondent_name', 'Respondent_Name'),
            ('consumer_attorney', 'Consumer_Attorney'),
            ('respondent_attorney', 'Respondent_Attorney'),
            ('disposition_type', 'Disposition_Type'),
            ('date_filed', 'Date_Filed'),
            ('date_closed', 'Date_Closed'),
            ('award_amount', 'Award_Amount'),
            ('claim_amount', 'Claim_Amount')
        ]:
            orig_col = self._map_column(df, field_type, source)
            if orig_col:
                processed_df[standard_col] = df[orig_col]
            else:
                # If column not found, add empty column
                processed_df[standard_col] = None
        
        # Add source column
        processed_df['Forum'] = source
        
        # Clean and standardize the data
        processed_df = self._clean_dataframe(processed_df)
        
        return processed_df
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize the dataframe.
        
        Args:
            df: Dataframe to clean
            
        Returns:
            Cleaned dataframe
        """
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Clean string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                # Replace NaN with None
                df[col] = df[col].replace({np.nan: None})
                
                # Clean strings
                df[col] = df[col].apply(lambda x: self._clean_string(x) if x is not None else None)
        
        # Convert date columns to datetime
        for date_col in ['Date_Filed', 'Date_Closed']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Convert numeric columns to float
        for num_col in ['Award_Amount', 'Claim_Amount']:
            if num_col in df.columns:
                df[num_col] = df[num_col].apply(self._extract_amount)
        
        # Clean and standardize disposition types
        if 'Disposition_Type' in df.columns:
            df['Disposition_Type'] = df['Disposition_Type'].apply(self._standardize_disposition)
        
        return df
    
    def _clean_string(self, value: Any) -> str:
        """
        Clean a string value.
        
        Args:
            value: Value to clean
            
        Returns:
            Cleaned string
        """
        if value is None:
            return None
        
        # Convert to string
        value = str(value)
        
        # Remove leading/trailing whitespace
        value = value.strip()
        
        # Replace multiple spaces with a single space
        value = re.sub(r'\s+', ' ', value)
        
        return value
    
    def _extract_amount(self, value: Any) -> Optional[float]:
        """
        Extract a numeric amount from a string.
        
        Args:
            value: Value to extract amount from
            
        Returns:
            Extracted amount as float, or None if not extractable
        """
        if value is None or pd.isna(value):
            return None
        
        # If already a number, return it
        if isinstance(value, (int, float)):
            return float(value)
        
        # Convert to string
        value = str(value)
        
        # Remove currency symbols, commas, etc.
        value = re.sub(r'[^\d.-]', '', value)
        
        # Try to convert to float
        try:
            return float(value)
        except:
            return None
    
    def _standardize_disposition(self, value: Optional[str]) -> Optional[str]:
        """
        Standardize disposition type.
        
        Args:
            value: Disposition type to standardize
            
        Returns:
            Standardized disposition type
        """
        if value is None:
            return None
        
        value = value.lower()
        
        # Standardize common disposition types
        if 'settled' in value or 'settlement' in value:
            return 'Settled'
        elif 'dismissed' in value and 'merit' in value:
            return 'Dismissed on the Merits'
        elif 'dismissed' in value or 'dismissal' in value:
            return 'Dismissed'
        elif 'withdrawn' in value:
            return 'Withdrawn'
        elif 'awarded' in value or 'award' in value:
            return 'Awarded'
        elif 'administrative' in value or 'admin' in value:
            return 'Administrative'
        else:
            # Return original value with first letter capitalized
            return value.capitalize()
    
    def _handle_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle duplicate cases in the dataframe.
        
        Args:
            df: Dataframe to process
            
        Returns:
            Dataframe with duplicates handled
        """
        # If Case_ID is available, use it to identify duplicates
        if 'Case_ID' in df.columns and not df['Case_ID'].isna().all():
            # Identify duplicates by Case_ID
            duplicates = df[df.duplicated(subset=['Case_ID'], keep=False)]
            
            if not duplicates.empty:
                # For each duplicate group, keep the most complete record
                for case_id in duplicates['Case_ID'].unique():
                    case_records = df[df['Case_ID'] == case_id]
                    
                    # Count non-null values in each record
                    completeness = case_records.notnull().sum(axis=1)
                    
                    # Keep the most complete record
                    most_complete_idx = completeness.idxmax()
                    to_drop = case_records.index.difference([most_complete_idx])
                    
                    # Drop less complete records
                    df = df.drop(to_drop)
        
        # Also check for potential duplicates using case name and arbitrator
        if 'Case_Name' in df.columns and 'Arbitrator_Name' in df.columns:
            # Identify potential duplicates
            potential_duplicates = df[df.duplicated(subset=['Case_Name', 'Arbitrator_Name'], keep=False)]
            
            if not potential_duplicates.empty:
                # For each potential duplicate group, keep the most complete record
                for _, group in potential_duplicates.groupby(['Case_Name', 'Arbitrator_Name']):
                    # Count non-null values in each record
                    completeness = group.notnull().sum(axis=1)
                    
                    # Keep the most complete record
                    most_complete_idx = completeness.idxmax()
                    to_drop = group.index.difference([most_complete_idx])
                    
                    # Drop less complete records
                    df = df.drop(to_drop)
        
        return df
    
    def _final_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform final cleaning operations on the combined dataframe.
        
        Args:
            df: Dataframe to clean
            
        Returns:
            Cleaned dataframe
        """
        # Reset index
        df = df.reset_index(drop=True)
        
        # Fill missing values with appropriate defaults
        for col in df.columns:
            if col in ['Case_ID', 'Case_Name', 'Arbitrator_Name', 'Respondent_Name',
                      'Consumer_Attorney', 'Respondent_Attorney', 'Disposition_Type']:
                df[col] = df[col].fillna('Unknown')
        
        # Ensure all required columns are present
        for col in self.standard_columns:
            if col not in df.columns:
                df[col] = None
        
        return df
