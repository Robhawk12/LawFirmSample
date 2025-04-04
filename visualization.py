import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List

def create_visualizations(data: pd.DataFrame) -> None:
    """
    Create visualizations for the dashboard.
    
    Args:
        data: Dataframe containing arbitration data
    """
    if data.empty:
        st.warning("No data available for visualization.")
        return
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Case count by Arbitrator (Top 10)
        st.subheader("Case Count by Arbitrator")
        arbitrator_counts = data['Arbitrator_Name'].value_counts().reset_index()
        arbitrator_counts.columns = ['Arbitrator_Name', 'Count']
        arbitrator_counts = arbitrator_counts.sort_values('Count', ascending=False).head(10)
        
        fig = px.bar(
            arbitrator_counts,
            x='Count',
            y='Arbitrator_Name',
            orientation='h',
            color='Count',
            color_continuous_scale='Blues',
            labels={'Count': 'Number of Cases', 'Arbitrator_Name': 'Arbitrator'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Case count by Respondent (Top 10)
        st.subheader("Case Count by Respondent (Company)")
        respondent_counts = data['Respondent_Name'].value_counts().reset_index()
        respondent_counts.columns = ['Respondent_Name', 'Count']
        respondent_counts = respondent_counts.sort_values('Count', ascending=False).head(10)
        
        fig = px.bar(
            respondent_counts,
            x='Count',
            y='Respondent_Name',
            orientation='h',
            color='Count',
            color_continuous_scale='Greens',
            labels={'Count': 'Number of Cases', 'Respondent_Name': 'Respondent (Company)'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Create two more columns for additional charts
    col3, col4 = st.columns(2)
    
    with col3:
        # Distribution of Case Dispositions
        st.subheader("Distribution of Case Dispositions")
        disposition_counts = data['Disposition_Type'].value_counts().reset_index()
        disposition_counts.columns = ['Disposition_Type', 'Count']
        
        fig = px.pie(
            disposition_counts,
            values='Count',
            names='Disposition_Type',
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Plasma
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col4:
        # Case count by Consumer Attorney (Top 10)
        st.subheader("Case Count by Consumer Attorney")
        attorney_counts = data['Consumer_Attorney'].value_counts().reset_index()
        attorney_counts.columns = ['Consumer_Attorney', 'Count']
        attorney_counts = attorney_counts.sort_values('Count', ascending=False).head(10)
        
        fig = px.bar(
            attorney_counts,
            x='Count',
            y='Consumer_Attorney',
            orientation='h',
            color='Count',
            color_continuous_scale='Reds',
            labels={'Count': 'Number of Cases', 'Consumer_Attorney': 'Consumer Attorney'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Create a single column for the timeline chart
    st.subheader("Case Timeline")
    
    if 'Date_Filed' in data.columns and not data['Date_Filed'].isna().all():
        # Group by date filed and count cases
        timeline_data = data.groupby(pd.Grouper(key='Date_Filed', freq='M')).size().reset_index()
        timeline_data.columns = ['Date', 'Count']
        
        fig = px.line(
            timeline_data,
            x='Date',
            y='Count',
            labels={'Count': 'Number of Cases', 'Date': 'Filing Date'},
            markers=True
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No date information available for timeline visualization.")
    
    # Create a section for forum-specific visualizations
    st.subheader("Forum Distribution")
    
    # Case count by Forum
    forum_counts = data['Forum'].value_counts().reset_index()
    forum_counts.columns = ['Forum', 'Count']
    
    fig = px.bar(
        forum_counts,
        x='Forum',
        y='Count',
        color='Forum',
        labels={'Count': 'Number of Cases', 'Forum': 'Arbitration Forum'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Create a section for additional details
    st.subheader("Additional Insights")
    
    # Claims and Awards
    if 'Claim_Amount' in data.columns and not data['Claim_Amount'].isna().all():
        col5, col6 = st.columns(2)
        
        with col5:
            # Claim Amount Distribution
            st.markdown("#### Claim Amount Distribution")
            
            claim_data = data[data['Claim_Amount'].notna()].copy()
            claim_data['Claim_Bracket'] = pd.cut(
                claim_data['Claim_Amount'],
                bins=[0, 1000, 5000, 10000, 50000, 100000, float('inf')],
                labels=['0-1K', '1K-5K', '5K-10K', '10K-50K', '50K-100K', '100K+']
            )
            
            claim_counts = claim_data['Claim_Bracket'].value_counts().reset_index()
            claim_counts.columns = ['Claim_Bracket', 'Count']
            
            fig = px.bar(
                claim_counts,
                x='Claim_Bracket',
                y='Count',
                color='Count',
                color_continuous_scale='Viridis',
                labels={'Count': 'Number of Cases', 'Claim_Bracket': 'Claim Amount Range'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col6:
            # Award Amount Distribution (if available)
            st.markdown("#### Award Amount Distribution")
            
            if 'Award_Amount' in data.columns and not data['Award_Amount'].isna().all():
                award_data = data[data['Award_Amount'].notna()].copy()
                award_data['Award_Bracket'] = pd.cut(
                    award_data['Award_Amount'],
                    bins=[0, 1000, 5000, 10000, 50000, 100000, float('inf')],
                    labels=['0-1K', '1K-5K', '5K-10K', '10K-50K', '50K-100K', '100K+']
                )
                
                award_counts = award_data['Award_Bracket'].value_counts().reset_index()
                award_counts.columns = ['Award_Bracket', 'Count']
                
                fig = px.bar(
                    award_counts,
                    x='Award_Bracket',
                    y='Count',
                    color='Count',
                    color_continuous_scale='Cividis',
                    labels={'Count': 'Number of Cases', 'Award_Bracket': 'Award Amount Range'}
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No award amount data available for visualization.")
    else:
        st.info("No claim amount data available for visualization.")

def create_arbitrator_visualization(data: pd.DataFrame, arbitrator_name: str) -> None:
    """
    Create visualizations for a specific arbitrator.
    
    Args:
        data: Dataframe containing arbitration data
        arbitrator_name: Name of the arbitrator to visualize
    """
    # Filter data for the specified arbitrator
    arbitrator_data = data[data['Arbitrator_Name'] == arbitrator_name]
    
    if arbitrator_data.empty:
        st.warning(f"No data available for arbitrator: {arbitrator_name}")
        return
    
    # Display basic stats
    st.metric("Total Cases", len(arbitrator_data))
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Disposition distribution for this arbitrator
        st.subheader("Case Disposition Distribution")
        disp_counts = arbitrator_data['Disposition_Type'].value_counts().reset_index()
        disp_counts.columns = ['Disposition_Type', 'Count']
        
        fig = px.pie(
            disp_counts,
            values='Count',
            names='Disposition_Type',
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Plasma
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Respondent companies for this arbitrator
        st.subheader("Top Respondent Companies")
        resp_counts = arbitrator_data['Respondent_Name'].value_counts().reset_index()
        resp_counts.columns = ['Respondent_Name', 'Count']
        resp_counts = resp_counts.head(10)
        
        fig = px.bar(
            resp_counts,
            x='Count',
            y='Respondent_Name',
            orientation='h',
            color='Count',
            color_continuous_scale='Greens',
            labels={'Count': 'Number of Cases', 'Respondent_Name': 'Respondent (Company)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Display additional details if available
    if 'Claim_Amount' in arbitrator_data.columns and not arbitrator_data['Claim_Amount'].isna().all():
        st.subheader("Claim and Award Amounts")
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Average claim amount
            avg_claim = arbitrator_data['Claim_Amount'].mean()
            st.metric("Average Claim Amount", f"${avg_claim:,.2f}")
            
            # Claim amount distribution
            fig = px.box(
                arbitrator_data,
                y='Claim_Amount',
                labels={'Claim_Amount': 'Claim Amount ($)'},
                title='Claim Amount Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            # Average award amount (if available)
            if 'Award_Amount' in arbitrator_data.columns and not arbitrator_data['Award_Amount'].isna().all():
                avg_award = arbitrator_data['Award_Amount'].mean()
                st.metric("Average Award Amount", f"${avg_award:,.2f}")
                
                # Award amount distribution
                fig = px.box(
                    arbitrator_data[arbitrator_data['Award_Amount'].notna()],
                    y='Award_Amount',
                    labels={'Award_Amount': 'Award Amount ($)'},
                    title='Award Amount Distribution'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No award amount data available for this arbitrator.")

def create_respondent_visualization(data: pd.DataFrame, respondent_name: str) -> None:
    """
    Create visualizations for a specific respondent.
    
    Args:
        data: Dataframe containing arbitration data
        respondent_name: Name of the respondent to visualize
    """
    # Filter data for the specified respondent
    respondent_data = data[data['Respondent_Name'] == respondent_name]
    
    if respondent_data.empty:
        st.warning(f"No data available for respondent: {respondent_name}")
        return
    
    # Display basic stats
    st.metric("Total Cases", len(respondent_data))
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Disposition distribution for this respondent
        st.subheader("Case Disposition Distribution")
        disp_counts = respondent_data['Disposition_Type'].value_counts().reset_index()
        disp_counts.columns = ['Disposition_Type', 'Count']
        
        fig = px.pie(
            disp_counts,
            values='Count',
            names='Disposition_Type',
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Plasma
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Arbitrators for this respondent
        st.subheader("Top Arbitrators")
        arb_counts = respondent_data['Arbitrator_Name'].value_counts().reset_index()
        arb_counts.columns = ['Arbitrator_Name', 'Count']
        arb_counts = arb_counts.head(10)
        
        fig = px.bar(
            arb_counts,
            x='Count',
            y='Arbitrator_Name',
            orientation='h',
            color='Count',
            color_continuous_scale='Blues',
            labels={'Count': 'Number of Cases', 'Arbitrator_Name': 'Arbitrator'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Display additional details if available
    if 'Claim_Amount' in respondent_data.columns and not respondent_data['Claim_Amount'].isna().all():
        st.subheader("Claim and Award Amounts")
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Average claim amount
            avg_claim = respondent_data['Claim_Amount'].mean()
            st.metric("Average Claim Amount", f"${avg_claim:,.2f}")
            
            # Claim amount distribution
            fig = px.box(
                respondent_data,
                y='Claim_Amount',
                labels={'Claim_Amount': 'Claim Amount ($)'},
                title='Claim Amount Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col4:
            # Average award amount (if available)
            if 'Award_Amount' in respondent_data.columns and not respondent_data['Award_Amount'].isna().all():
                avg_award = respondent_data['Award_Amount'].mean()
                st.metric("Average Award Amount", f"${avg_award:,.2f}")
                
                # Award amount distribution
                fig = px.box(
                    respondent_data[respondent_data['Award_Amount'].notna()],
                    y='Award_Amount',
                    labels={'Award_Amount': 'Award Amount ($)'},
                    title='Award Amount Distribution'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No award amount data available for this respondent.")
