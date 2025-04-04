"""
Database module for the Arbitration Data Visualization Tool.
This module handles database operations for storing and retrieving arbitration data.
"""

import os
from typing import Dict, List, Optional, Union, Any
import pandas as pd
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, 
    Date, Text, Boolean, MetaData, Table, inspect
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.sql import select, delete

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
            # Map DataFrame columns to database columns
            db_data = self._map_dataframe_to_db(data)
            
            # Insert data into the database
            with self.engine.begin() as conn:
                # For each row, check if it already exists by case_id
                inserted = 0
                updated = 0
                
                for _, row in db_data.iterrows():
                    case_id = row['case_id']
                    
                    # Check if case already exists
                    stmt = select(self.arbitration_cases).where(
                        self.arbitration_cases.c.case_id == case_id
                    )
                    existing = conn.execute(stmt).fetchone()
                    
                    if existing:
                        # Update existing case
                        stmt = self.arbitration_cases.update().where(
                            self.arbitration_cases.c.case_id == case_id
                        ).values(**{k: v for k, v in row.items() if k != 'case_id'})
                        conn.execute(stmt)
                        updated += 1
                    else:
                        # Insert new case
                        conn.execute(self.arbitration_cases.insert().values(**row))
                        inserted += 1
            
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
    
    def load_data(self, filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Load arbitration data from the database.
        
        Args:
            filters: Dictionary of filters to apply
            
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
            
            # Execute query and convert to DataFrame
            with self.engine.connect() as conn:
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
                # Get total row count
                row_count = conn.execute(
                    select([self.arbitration_cases])
                ).rowcount
                
                # Get unique arbitrators count
                arb_count = conn.execute(
                    select([self.arbitration_cases.c.arbitrator_name]).distinct()
                ).rowcount
                
                # Get unique respondents count
                resp_count = conn.execute(
                    select([self.arbitration_cases.c.respondent_name]).distinct()
                ).rowcount
                
                return {
                    'status': 'success',
                    'total_cases': row_count,
                    'unique_arbitrators': arb_count,
                    'unique_respondents': resp_count
                }
        
        except SQLAlchemyError as e:
            return {
                'status': 'error',
                'message': str(e)
            }