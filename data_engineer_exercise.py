# Print message to indicate that script is running.
print('Data engineering has begun...')

# Import dependencies
import pandas as pd
import datetime as dt

# Open cons.csv file and read as pandas DataFrame
cons_path = "Resources/cons.csv"
cons_df = pd.read_csv(cons_path)

# Client requested dates to be in datetime data type.
# Convert create_dt and modified_dt from string to datetime
cons_df['create_dt'] = pd.to_datetime(cons_df['create_dt'], format='%a, %Y-%m-%d %H:%M:%S')
cons_df['modified_dt'] = pd.to_datetime(cons_df['modified_dt'], format='%a, %Y-%m-%d %H:%M:%S')

# Limit DataFrame to specific columns
cons_df = cons_df[['cons_id', 'source', 'create_dt', 'modified_dt']]

# Open cons_email.csv file and read as pandas DataFrame
email_path = "Resources/cons_email.csv"
email_df = pd.read_csv(email_path)

# Client requested primary email only.
# Filter DataFrame by primary email.
primary_email_df = email_df.loc[email_df['is_primary'] == 1, ['cons_email_id', 'cons_id', 'email']]

# Open cons_email_chapter_subscription.csv file and read as pandas DataFrame
chapter_sub_path = "Resources/cons_email_chapter_subscription.csv"
chapter_sub_df = pd.read_csv(chapter_sub_path)

# Client wrote, "We only care about subscription statuses where chapter_id is 1."
# Filter DataFrame where chapter_id is 1.
chapter1_sub_df = chapter_sub_df.loc[chapter_sub_df['chapter_id'] == 1, ['cons_email_id', 'isunsub']]

# Merge constiuents DataFrame with primary email DataFrame.
cons_and_email_df = pd.merge(cons_df, primary_email_df, how='inner', on='cons_id')

# Merge newly merged DataFrame with chapter subscriptions DataFrame to create "people" DataFrame
# Client wrote, "If an email is not present in this table, it is assumed to still be subscribed where chapter_id is 1."
# Use a left join to keep all emails from previously merged data in final DataFrame.
people_df = pd.merge(cons_and_email_df, chapter1_sub_df, how='left', on='cons_email_id')

# Convert NaN values to False per client instructions ("it is assummed...").
people_df['isunsub'] = people_df['isunsub'].fillna(0)
people_df['isunsub'] = people_df['isunsub'].astype(bool)

# Organize and limit DataFrame to requested columns
people_df = people_df[['email', 'source', 'isunsub', 'create_dt', 'modified_dt']]

# Rename columns to requested column names
people_df.rename(columns={'source': 'code', 'isunsub': 'is_unsub', 'create_dt': 'created_dt', 'modified_dt': 'updated_dt'}, inplace=True)

# Export DataFrame to .csv file
people_df.to_csv('Output/people.csv')

# Convert DateTime to Date per client schema.
people_df['created_dt'] = pd.to_datetime(people_df['created_dt']).dt.date

# Count number of constituents acquired on each acquisition date.
acquisition_facts_df = pd.DataFrame(people_df.groupby('created_dt').count()['email'])
acquisition_facts_df = acquisition_facts_df.reset_index()

# Rename columns to requested column names
acquisition_facts_df.rename(columns={'created_dt': 'acquisition_date', 'email': 'acquisition'}, inplace=True)

# Export DataFrame to .csv file
acquisition_facts_df.to_csv('Output/acquisition_facts.csv')

# Print message to indicate that the script completed without error.
print('Data engineering has completed successfully.')