import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Read CSV file into DataFrame
df = pd.read_csv('my_data.csv')
print(df.head())
""" 
with sns.axes_style('whitegrid'):
    plt.figure(figsize=(8, 4), dpi=200)
    ax = sns.scatterplot(data=df,
                         x='Min',
                         y='Max',
                         #scatter_kws={'alpha': 0.4},
                         #line_kws={'color': 'black'},
                         hue='Min', # change colour
                         size='Max',) # change size of dot

    ax.set(ylim=(0, 20),
           xlim=(0, 20),
           ylabel='Min',
           xlabel='Max',)

plt.show()
"""
with sns.axes_style('darkgrid'):
    plt.figure(figsize=(8, 4), dpi=200)
    ax = sns.regplot(data=df,
                     x='Min',
                     y='Max',
                     color='#2f4b7c',
                     scatter_kws={'alpha': 0.3},
                     line_kws={'color': '#ff7c43'})

    ax.set(ylim=(0, 20),
           xlim=(0, 20),
           ylabel='Min',
           xlabel='Max', )

plt.show()
