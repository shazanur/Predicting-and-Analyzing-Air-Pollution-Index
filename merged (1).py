import pandas as pd

# Load the dataset
df = pd.read_csv('cars_2017.csv')
df2 = pd.read_csv('cars_2018.csv')
df3 = pd.read_csv('cars_2019.csv')
df4 = pd.read_csv('cars_2020.csv')
df5 = pd.read_csv('cars_2021.csv')
df6 = pd.read_csv('cars_2022.csv')
# Convert the 'date_reg' column to datetime objects
df['date_reg'] = pd.to_datetime(df['date_reg'])
df2['date_reg'] = pd.to_datetime(df2['date_reg'])
df3['date_reg'] = pd.to_datetime(df3['date_reg'])
df4['date_reg'] = pd.to_datetime(df4['date_reg'])
df5['date_reg'] = pd.to_datetime(df5['date_reg'])
df6['date_reg'] = pd.to_datetime(df6['date_reg'])

# Combine all dataframes into a single dataframe
df = pd.concat([df, df2, df3, df4, df5, df6], ignore_index=True)

# Extract the month and year for grouping (formatted as YYYY-MM)
df['month'] = df['date_reg'].dt.to_period('M')

# Count the number of registrations per month
monthly_counts = df.groupby('month').size().reset_index(name='car_registrations')

# Display the results
print(monthly_counts)

# Save the merged dataframe to a new CSV file
#monthly_counts.to_csv('car_registered.csv', index=False)

#for air pollution analysis
air=pd.read_csv('air_pollution.csv')
air['date'] = pd.to_datetime(air['date'])
air['month'] = air['date'].dt.to_period('M')

#for electricity consumption
electricity=pd.read_csv('electricity_consumption (1).csv')
electricity['date'] = pd.to_datetime(electricity['date'])
electricity['month'] = electricity['date'].dt.to_period('M')
#filter only 'total' sector data
electricity = electricity[electricity['sector'] == 'total']

#for ipi index
ipi=pd.read_csv('ipi.csv')
ipi['date'] = pd.to_datetime(ipi['date'])
ipi['month'] = ipi['date'].dt.to_period('M')
#filter only 'abs' series data
ipi = ipi[ipi['series'] == 'abs']

#for rainfall data
rainfall=pd.read_csv('rainfall.csv')
#merge month and day columns to create a date column
rainfall['date'] = pd.to_datetime(rainfall[['Month', 'Day', 'Year']])
rainfall['date'] = pd.to_datetime(rainfall['date'])
rainfall['month'] = rainfall['date'].dt.to_period('M')
#calculate average rainfall per month
rainfall = rainfall.groupby('month')['Rainfall'].mean().reset_index()

#for fire data
fire=pd.read_csv('fire.csv')
fire['acq_date'] = pd.to_datetime(fire['acq_date'])
fire['month'] = fire['acq_date'].dt.to_period('M')
#filter only 'BRIGHTNESS' and 'FRP' columns
fire = fire[['month', 'brightness', 'frp']]
#calculate average brightness and frp per month
fire = fire.groupby('month').agg({'brightness': 'mean', 'frp': 'mean'}).reset_index()

#for car registrations
car_registered=pd.read_csv('car_registered.csv')
car_registered['month'] = pd.to_datetime(car_registered['month']).dt.to_period('M')

# Merge all dataframes on 'month'
merged_df = monthly_counts.merge(air[['month', 'concentration', 'pollutant']], on='month', how='left')
merged_df = merged_df.merge(electricity[['month', 'consumption']], on='month', how='left')
merged_df = merged_df.merge(ipi[['month', 'index']], on='month', how='left')
merged_df = merged_df.merge(rainfall[['month', 'Rainfall']], on='month', how='left')
merged_df = merged_df.merge(fire[['month', 'brightness', 'frp']], on ='month', how='left')
merged_df = merged_df.merge(car_registered[['month', 'car_registrations']], on='month', how='left')
# Rename columns for clarity
merged_df.rename(columns={
    'Rainfall': 'avg_rainfall_mm',
    'brightness': 'fire_brightness',
    'frp': 'fire_frp',
    'car_registrations': 'vehicles_count',
    'index': 'ipi_index',
    'concentration': 'air_pollution_concentration',
    'pollutant': 'air_pollutant_type'
}, inplace=True)

print(merged_df)
# Save the merged dataframe to a new CSV file
merged_df.to_csv('new_data.csv', index=False)


