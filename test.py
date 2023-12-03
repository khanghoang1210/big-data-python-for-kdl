import pandas as pd
import numpy as np

# Mock data generation
np.random.seed(23) # For reproducibility

# Sample size
n_samples = 100

# Generate movie titles
movie_titles = [f'Movie {i}' for i in range(1, n_samples + 1)]

# Generate rank change percentages
rank_change_percentages = np.random.uniform(-50, 50, n_samples)

# Generate gross change percentages
gross_change_percentages = np.random.uniform(-100, 100, n_samples)

# Generate revenue (in millions)
revenues = np.random.uniform(1, 500, n_samples)

# Generate ratings
ratings = np.random.uniform(1, 10, n_samples)

# Create DataFrame
df_movies = pd.DataFrame({
    'Title': movie_titles,
    'Rank_Change_%': rank_change_percentages,
    'Gross_Change_%': gross_change_percentages,
    'Revenue_Millions': revenues,
    'Rating': ratings
})

# Show the head of the dataframe
df_movies.head()

# Data Cleaning: Check for missing values and data types

# Check for missing values
missing_values = df_movies.isnull().sum()

# Check data types
data_types = df_movies.dtypes

# Output the results
print('Missing Values:\n', missing_values)
print('\nData Types:\n', data_types)

import matplotlib.pyplot as plt
import seaborn as sns

# Set the aesthetic style of the plots
sns.set_style('whitegrid')

# Initialize the figure
plt.figure(figsize=(14, 7))

# Revenue vs. Rating Scatter Plot
sns.scatterplot(data=df_movies, x='Revenue_Millions', y='Rating', hue='Rank_Change_%', palette='coolwarm', size='Gross_Change_%', sizes=(20, 200))
plt.title('Revenue vs. Rating with Rank and Gross Change Indicators')
plt.xlabel('Revenue (Millions)')
plt.ylabel('Rating')
plt.legend(title='Rank Change % / Gross Change %')

# Show the plot
plt.show()