import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def load_sample_data() -> pd.DataFrame:
    """
    Generate sample arbitration data for demonstration purposes.
    
    Returns:
        DataFrame containing sample arbitration data
    """
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Define sample data parameters
    num_records = 500
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    # Sample arbitrators
    arbitrators = [
        "John L. Smith Esq.", "Maria A. Johnson", "Robert T. Williams", 
        "Jane D. Brown", "Michael S. Davis", "Sarah E. Wilson", 
        "David M. Taylor", "Elizabeth R. Anderson", "James C. Thomas",
        "Patricia N. White", "Thomas O. Harris", "Jennifer P. Martin",
        "Richard Q. Thompson", "Rebecca S. Garcia", "Joseph T. Martinez"
    ]
    
    # Sample respondents (companies)
    respondents = [
        "Coinbase Inc.", "Bank of America", "Wells Fargo", "Citibank", 
        "Amazon.com Inc.", "PayPal Inc.", "Crypto.com LLC", "Uber Technologies Inc.",
        "AT&T Corp.", "Verizon Services", "Chase Bank", "Capital One", 
        "Apple Inc.", "Google LLC", "MetLife Inc."
    ]
    
    # Sample consumer attorneys
    consumer_attorneys = [
        "Ian Campbell", "Michael Saunders", "Lisa Chen Law Firm",
        "Rodriguez & Associates", "Patel Legal Services", "Connor Williams",
        "Law Office of Matthew G. Jordan", "Emily Cox Partners", "Goldstein Law Inc.",
        "Sharma & Lee", "Jason Nguyen", "Tyler Green Legal"
    ]
    
    # Sample respondent attorneys
    respondent_attorneys = [
        "Morgan Lewis LLP", "Sidley Law LLC", "O'Melveny & Myers",
        "Gibson Dunn", "Skadden Arps", "Latham & Watkins LLP",
        "Davis Polk", "Sullivan & Cromwell", "Kirkland & Ellis",
        "Baker McKenzie", "White & Case", "DLA Piper"
    ]
    
    # Sample disposition types
    disposition_types = [
        "Settled", "Administrative", "Withdrawn", "Awarded", "Dismissed on the Merits"
    ]
    disposition_weights = [0.55, 0.24, 0.15, 0.05, 0.01]  # Based on screenshot data
    
    # Sample arbitration forums
    forums = ["AAA", "JAMS"]
    forum_weights = [0.65, 0.35]  # Preference for AAA as mentioned in PRD
    
    # Generate random dates
    date_range = (end_date - start_date).days
    filing_dates = [start_date + timedelta(days=random.randint(0, date_range)) for _ in range(num_records)]
    filing_dates.sort()  # Sort dates chronologically
    
    # Calculate closing dates (1-12 months after filing)
    closing_dates = [filing_date + timedelta(days=random.randint(30, 365)) for filing_date in filing_dates]
    
    # Generate sample data
    data = {
        'Case_ID': [f"ARB-{2018+i//100}-{1000+i}" for i in range(num_records)],
        'Arbitrator_Name': [random.choice(arbitrators) for _ in range(num_records)],
        'Respondent_Name': [random.choice(respondents) for _ in range(num_records)],
        'Consumer_Attorney': [random.choice(consumer_attorneys) for _ in range(num_records)],
        'Respondent_Attorney': [random.choice(respondent_attorneys) for _ in range(num_records)],
        'Disposition_Type': [random.choices(disposition_types, weights=disposition_weights)[0] for _ in range(num_records)],
        'Date_Filed': filing_dates,
        'Date_Closed': closing_dates,
        'Forum': [random.choices(forums, weights=forum_weights)[0] for _ in range(num_records)]
    }
    
    # Generate claim amounts (lognormal distribution for realistic amounts)
    claim_amounts = np.round(np.random.lognormal(mean=9.5, sigma=1.2, size=num_records), 2)
    data['Claim_Amount'] = claim_amounts
    
    # Generate award amounts (only for "Awarded" cases, typically less than claim amount)
    award_amounts = []
    for i in range(num_records):
        if data['Disposition_Type'][i] == "Awarded":
            # Award typically 30-90% of claim amount
            award_percent = random.uniform(0.3, 0.9)
            award_amounts.append(round(data['Claim_Amount'][i] * award_percent, 2))
        else:
            award_amounts.append(None)
    data['Award_Amount'] = award_amounts
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Add some correlations to make data more realistic
    
    # Larger companies tend to have specific law firms
    for i, respondent in enumerate(respondents[:5]):  # Top 5 companies
        mask = df['Respondent_Name'] == respondent
        df.loc[mask, 'Respondent_Attorney'] = respondent_attorneys[i]
    
    # Certain arbitrators tend to handle specific types of cases
    for i, arbitrator in enumerate(arbitrators[:5]):  # Top 5 arbitrators
        mask = df['Arbitrator_Name'] == arbitrator
        df.loc[mask, 'Disposition_Type'] = np.random.choice(
            disposition_types,
            size=mask.sum(),
            p=[0.6, 0.2, 0.1, 0.05, 0.05]  # Higher settlement rate
        )
    
    # Add some cases with the same respondent and arbitrator for realistic patterns
    common_pairs = [
        (arbitrators[0], respondents[0]),
        (arbitrators[1], respondents[1]),
        (arbitrators[2], respondents[2])
    ]
    
    for arb, resp in common_pairs:
        mask = (df['Arbitrator_Name'] == arb) & (df['Respondent_Name'] == resp)
        if mask.sum() < 10:  # Ensure at least 10 cases with this pair
            additional_needed = 10 - mask.sum()
            for i in range(min(additional_needed, 20)):
                idx = random.randint(0, num_records-1)
                df.loc[idx, 'Arbitrator_Name'] = arb
                df.loc[idx, 'Respondent_Name'] = resp
    
    # Add some blank values to simulate real-world data inconsistencies
    for col in ['Respondent_Attorney', 'Consumer_Attorney', 'Award_Amount']:
        mask = np.random.choice([True, False], size=num_records, p=[0.05, 0.95])
        df.loc[mask, col] = None
    
    return df
