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
"""
To display Pandas Seaborn plots on a website using HTML, you can follow these steps:

Create your plot using Pandas Seaborn:

python
Copy code
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="whitegrid")
tips = sns.load_dataset("tips")
ax = sns.barplot(x="day", y="total_bill", data=tips)
plt.savefig("my_plot.png")
This code will save your plot as a PNG file called "my_plot.png" in the same directory as your Python script.

Create an HTML file with an <img> tag that references your plot:

html
Copy code
<html>
  <head>
    <title>My Plot</title>
  </head>
  <body>
    <img src="my_plot.png" alt="My Plot">
  </body>
</html>
This code creates an HTML file with an <img> tag that references the "my_plot.png" file you created in step 1.

Open the HTML file in your web browser:

You can open the HTML file in your web browser by double-clicking on it or by using your web server to serve the file.

If you're using a web server, you can copy the HTML file and the "my_plot.png" file to your web server's document root directory and then navigate to the HTML file's URL in your web browser.

For example, if your web server's document root directory is "/var/www/html" and you've copied the HTML file and the "my_plot.png" file to "/var/www/html/my_directory", you can navigate to "http://localhost/my_directory/my_plot.html" in your web browser to view the plot.


"""