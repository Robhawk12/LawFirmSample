"""
Database module for the Arbitration Data Visualization Tool.
This module handles database operations for storing and retrieving arbitration data.
"""

import os
from typing import Dict, List, Optional, Union, Any
import pandas as pd
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, 
    Date, Text, Boolean, MetaData, Table, inspect,
    func, distinct, select, delete
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine

# Get database URL from environment variable
DATABASE_URL = os.environ.get('DATABASE_URL')

class ArbitrationDatabase:
    """Class for managing arbitration data in a PostgreSQL database."""
    
    def __init__(self):
        """Initialize the database connection."""
        self.engine = create_engine(DATABASE_URL)
        self.metadata = MetaData()
        self.arbitration_cases = self._define_table()
        self.metadata.create_all(self.engine)
    
    def _define_table(self) -> Table:
        """
        Define the arbitration_cases table structure.
        
        Returns:
            Table object for arbitration_cases
        """
        # Define the arbitration_cases table if it doesn't exist
        arbitration_cases = Table(
            'arbitration_cases',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('case_id', String(100), unique=True, nullable=False),
            Column('date_filed', Date),
            Column('respondent_name', String(255)),
            Column('consumer_rep_firm', String(255)),
            Column('consumer_attorney', String(255)),
            Column('arbitrator_name', String(255)),
            Column('forum', String(100)),
            Column('claim_amount', Float),
            Column('award_amount', Float),
            Column('disposition_type', String(100)),
            Column('consumer_prevailed', Boolean),
            Column('business_prevailed', Boolean),
            Column('decision_date', Date),
            Column('case_duration_days', Integer),
            Column('source', String(50)),  # AAA or JAMS
            Column('notes', Text),
            extend_existing=True
        )
        return arbitration_cases
    
    def table_exists(self) -> bool:
        """
        Check if the arbitration_cases table exists.
        
        Returns:
            True if the table exists, False otherwise
        """
        inspector = inspect(self.engine)
        return 'arbitration_cases' in inspector.get_table_names()
    
    def save_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Save arbitration data to the database.
        
        Args:
            data: DataFrame containing arbitration data
            
        Returns:
            Dictionary with status and details
        """
        try:
            # Map DataFrame columns to database columns if needed
            if 'case_id' not in data.columns and 'Case_ID' in data.columns:
                db_data = self._map_dataframe_to_db(data)
            else:
                db_data = data.copy()
            
            # Insert data into the database
            with self.engine.begin() as conn:
                inserted = 0
                updated = 0
                
                # For efficient data processing, gather all case_ids at once
                all_case_ids = [str(cid) if cid is not None else None for cid in db_data['case_id']]
                
                # Check which case_ids already exist in the database in one query
                existing_ids_query = select(self.arbitration_cases.c.case_id).where(
                    self.arbitration_cases.c.case_id.in_(all_case_ids)
                )
                existing_ids_result = conn.execute(existing_ids_query).fetchall()
                existing_ids = {row[0] for row in existing_ids_result}
                
                # Process in batches for better performance
                batch_size = 250  # Adjust based on the size of each record
                
                # Prepare records for insert and update
                records_to_insert = []
                
                # First pass: collect records to insert (won't exist in DB)
                for _, row in db_data.iterrows():
                    case_id = row['case_id']
                    case_id_str = str(case_id) if case_id is not None else None
                    
                    if case_id_str not in existing_ids:
                        # Convert row to dictionary and ensure case_id is a string
                        row_dict = {k: (str(v) if k == 'case_id' and v is not None else v) 
                                   for k, v in row.items() if not pd.isna(v)}
                        records_to_insert.append(row_dict)
                
                # Batch inserts for better performance
                for i in range(0, len(records_to_insert), batch_size):
                    batch = records_to_insert[i:i+batch_size]
                    if batch:  # Skip empty batches
                        conn.execute(self.arbitration_cases.insert(), batch)
                        inserted += len(batch)
                
                # Second pass: update existing records
                for _, row in db_data.iterrows():
                    case_id = row['case_id']
                    case_id_str = str(case_id) if case_id is not None else None
                    
                    if case_id_str in existing_ids:
                        # Update existing record
                        # Remove NaN values and case_id from update
                        row_dict = {k: v for k, v in row.items() 
                                   if k != 'case_id' and not pd.isna(v)}
                        
                        if row_dict:  # Only update if there are values to update
                            stmt = self.arbitration_cases.update().where(
                                self.arbitration_cases.c.case_id == case_id_str
                            ).values(**row_dict)
                            conn.execute(stmt)
                            updated += 1
            
            return {
                'status': 'success',
                'inserted': inserted,
                'updated': updated,
                'total': len(db_data)
            }
        
        except SQLAlchemyError as e:
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def _map_dataframe_to_db(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map DataFrame columns to database columns.
        
        Args:
            df: DataFrame to map
            
        Returns:
            Mapped DataFrame
        """
        # Create a copy to avoid modifying the original
        db_df = pd.DataFrame()
        
        # Map column names (case-insensitive)
        column_mapping = {
            'Case_ID': 'case_id',
            'Date_Filed': 'date_filed',
            'Respondent_Name': 'respondent_name',
            'Consumer_Rep_Firm': 'consumer_rep_firm',
            'Consumer_Attorney': 'consumer_attorney',
            'Arbitrator_Name': 'arbitrator_name',
            'Forum': 'forum',
            'Claim_Amount': 'claim_amount',
            'Award_Amount': 'award_amount',
            'Disposition_Type': 'disposition_type',
            'Consumer_Prevailed': 'consumer_prevailed',
            'Business_Prevailed': 'business_prevailed',
            'Decision_Date': 'decision_date',
            'Case_Duration_Days': 'case_duration_days',
            'Source': 'source',
            'Notes': 'notes'
        }
        
        # Map each column from the DataFrame to the database column
        for df_col, db_col in column_mapping.items():
            if df_col in df.columns:
                db_df[db_col] = df[df_col]
            else:
                # Check for case-insensitive match
                for col in df.columns:
                    if col.lower() == df_col.lower():
                        db_df[db_col] = df[col]
                        break
                else:
                    # Column not found, set to None
                    db_df[db_col] = None
        
        return db_df
    
    def load_data(self, filters: Optional[Dict[str, Any]] = None, chunk_size: int = 10000) -> pd.DataFrame:
        """
        Load arbitration data from the database.
        
        Args:
            filters: Dictionary of filters to apply
            chunk_size: Number of records to load at once for large datasets
            
        Returns:
            DataFrame containing arbitration data
        """
        try:
            # Build query based on filters
            query = select(self.arbitration_cases)
            
            if filters:
                for key, value in filters.items():
                    if hasattr(self.arbitration_cases.c, key) and value is not None:
                        query = query.where(getattr(self.arbitration_cases.c, key) == value)
            
            # Get total row count to determine if chunking is needed
            count_query = select([func.count()]).select_from(self.arbitration_cases)
            if filters:
                for key, value in filters.items():
                    if hasattr(self.arbitration_cases.c, key) and value is not None:
                        count_query = count_query.where(getattr(self.arbitration_cases.c, key) == value)
            
            # Execute query and convert to DataFrame
            with self.engine.connect() as conn:
                # Check if large dataset (>chunk_size rows)
                total_rows = conn.execute(count_query).scalar()
                
                if total_rows > chunk_size:
                    # For large datasets, load in chunks
                    all_chunks = []
                    
                    # Apply ordering to ensure consistent chunking
                    ordered_query = query.order_by(self.arbitration_cases.c.id)
                    
                    for offset in range(0, total_rows, chunk_size):
                        chunk_query = ordered_query.limit(chunk_size).offset(offset)
                        chunk_result = conn.execute(chunk_query)
                        chunk_data = pd.DataFrame(chunk_result.fetchall(), columns=chunk_result.keys())
                        all_chunks.append(chunk_data)
                    
                    if all_chunks:
                        data = pd.concat(all_chunks, ignore_index=True)
                    else:
                        data = pd.DataFrame()
                else:
                    # For smaller datasets, load all at once
                    result = conn.execute(query)
                    data = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            # Map database columns back to DataFrame columns
            return self._map_db_to_dataframe(data)
        
        except SQLAlchemyError as e:
            print(f"Error loading data from database: {e}")
            return pd.DataFrame()
    
    def _map_db_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map database columns to DataFrame columns.
        
        Args:
            df: DataFrame to map
            
        Returns:
            Mapped DataFrame
        """
        # Create a copy to avoid modifying the original
        app_df = pd.DataFrame()
        
        # Map column names (case-sensitive for app)
        column_mapping = {
            'case_id': 'Case_ID',
            'date_filed': 'Date_Filed',
            'respondent_name': 'Respondent_Name',
            'consumer_rep_firm': 'Consumer_Rep_Firm',
            'consumer_attorney': 'Consumer_Attorney',
            'arbitrator_name': 'Arbitrator_Name',
            'forum': 'Forum',
            'claim_amount': 'Claim_Amount',
            'award_amount': 'Award_Amount',
            'disposition_type': 'Disposition_Type',
            'consumer_prevailed': 'Consumer_Prevailed',
            'business_prevailed': 'Business_Prevailed',
            'decision_date': 'Decision_Date',
            'case_duration_days': 'Case_Duration_Days',
            'source': 'Source',
            'notes': 'Notes'
        }
        
        # Map each column from the database to the DataFrame column
        for db_col, app_col in column_mapping.items():
            if db_col in df.columns:
                app_df[app_col] = df[db_col]
        
        return app_df
    
    def clear_data(self) -> Dict[str, Any]:
        """
        Clear all data from the arbitration_cases table.
        
        Returns:
            Dictionary with status and details
        """
        try:
            with self.engine.begin() as conn:
                stmt = delete(self.arbitration_cases)
                result = conn.execute(stmt)
                
                return {
                    'status': 'success',
                    'rows_deleted': result.rowcount
                }
        
        except SQLAlchemyError as e:
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Returns:
            Dictionary with database statistics
        """
        try:
            with self.engine.connect() as conn:
                # Use SQL COUNT for better performance on large tables
                # Get total row count using COUNT
                count_query = select([
                    func.count().label('total_count'),
                    func.count(distinct(self.arbitration_cases.c.arbitrator_name)).label('arb_count'),
                    func.count(distinct(self.arbitration_cases.c.respondent_name)).label('resp_count')
                ]).select_from(self.arbitration_cases)
                
                result = conn.execute(count_query).fetchone()
                
                if result:
                    return {
                        'status': 'success',
                        'total_cases': result['total_count'],
                        'unique_arbitrators': result['arb_count'],
                        'unique_respondents': result['resp_count']
                    }
                else:
                    return {
                        'status': 'success',
                        'total_cases': 0,
                        'unique_arbitrators': 0,
                        'unique_respondents': 0
                    }
        
        except SQLAlchemyError as e:
            return {
                'status': 'error',
                'message': str(e)
            }