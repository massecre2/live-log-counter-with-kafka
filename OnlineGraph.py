import matplotlib as mpl
mpl.use('WebAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime


# Create figure for plotting
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
xs,ys,ys2,ys3,ys4,ys5   = [] , [] , [] , [] , [] , [] 

# This function is called periodically from FuncAnimation
def animate(i, xs, ys, ys2, ys3, ys4, ys5):
    time_data = open(r'C:\Users\OGUZ\Desktop\TEB\RealTime\time.txt').read()
    x_data = time_data.split('\n')
    
    for line in x_data:
        if len(line) > 1: 
            xs.append(datetime.strptime(str(line),'%Y-%m-%d %H:%M:%S'))


    with open(r'C:\Users\OGUZ\Desktop\TEB\RealTime\moscow.txt','r') as value:
        for line in value:
            ys.append(int(line))
    
    with open(r'C:\Users\OGUZ\Desktop\TEB\RealTime\istanbul.txt','r') as value:
        for line in value:
            ys2.append(int(line))
            
    with open(r'C:\Users\OGUZ\Desktop\TEB\RealTime\tokyo.txt','r') as value:
        for line in value:
            ys3.append(int(line))

    with open(r'C:\Users\OGUZ\Desktop\TEB\RealTime\beijing.txt','r') as value:
        for line in value:
            ys4.append(int(line))

    with open(r'C:\Users\OGUZ\Desktop\TEB\RealTime\london.txt','r') as value:
        for line in value:
            ys5.append(int(line))


    # Limit x and y lists to 20 items
    xs = xs[-20:]
    ys = ys[-20:]
    ys2 = ys2[-20:]
    ys3 = ys3[-20:]
    ys4 = ys4[-20:]
    ys5 = ys5[-20:]    
    
    # Draw x and y lists
    ax.clear()
    ax.plot(xs, ys , label='moscow')
    ax.plot(xs, ys2, label='istanbul')
    ax.plot(xs, ys3, label='tokyo')
    ax.plot(xs, ys4, label='beijing')    
    ax.plot(xs, ys5, label='london')    
    plt.legend()


    # Format plot
    plt.xticks(rotation=45, ha='right')
    plt.subplots_adjust(bottom=0.30)
    plt.title('ONLINE LOG COUNTER')
    plt.ylabel('TOTAL NUMBER OF LOG FROM SERVER')
    plt.ylim(0, 40)

# Set up plot to call animate() function periodically
ani = animation.FuncAnimation(fig, animate, fargs=(xs, ys, ys2, ys3, ys4, ys5), interval=1000)
plt.show()
