# importing matplotlib module 
from matplotlib import pyplot as plt 
import numpy as np

# x-axis values 
x = [5, 2, 9, 4, 7] 

# Y-axis values 
y = [10, 5, 8, 4, 2] 

# Function to plot 
plt.plot(x,y) 

# function to show the plot 
plt.show() 
####################################################
# x-axis values 
x = [5, 2, 9, 4, 7] 
  
# Y-axis values 
y = [10, 5, 8, 4, 2] 
  
# Function to plot the bar 
plt.bar(x,y) 
  
# function to show the plot 
plt.show() 
####################################################
bar_width = 0.35

# x-axis values 
x1 = [5, 10, 15, 20, 25] 
# Y-axis values 
y1 = [2, 5, 8, 12, 15] 

index = np.arange(5)

fig, ax = plt.subplots()
original = ax.bar(index, x1, bar_width,
                label="Original")

optimized = ax.bar(index+bar_width, y1,
                 bar_width, label="Optimized")


ax.set_xlabel('API Hits')
ax.set_ylabel('Counts')
ax.set_title('Pre-Import Optimization')
ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(["PB API Hits", "IX Hits", "Theft", "Public Order", "Drugs"])
ax.legend()

plt.show()